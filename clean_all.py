import datetime
import warnings
from typing import Any, Dict, List

import google
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger

SKIP_LABEL = "please-do-not-kill-me"

DiscoveryEndpoint = Any


class BaseDiscoveryClient:
    endpoint = None
    version = "v1"

    def __init__(self, project_id, credentials):
        self.credentials = credentials
        self.project_id = project_id

        if self.endpoint is None:
            raise Exception(
                "Client class has to have `endpoint` attribute set to discovery endpoint name"
            )

        self.client = build(self.endpoint, self.version, credentials=self.credentials)

    @staticmethod
    def is_stale(date: str) -> bool:
        try:
            today = datetime.datetime.today()
            time = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            today = datetime.datetime.now(tz=datetime.timezone.utc)
            time = datetime.datetime.fromisoformat(date)

        return time < today - datetime.timedelta(days=1)

    @staticmethod
    def _iterate(
        endpoint: DiscoveryEndpoint, key: str = "items", **kwargs
    ) -> List[Dict]:
        """
        Iterates through endpoint.list(...).execute() discovery API endpoint

        :param endpoint: An discovery API object for example ``self.client.instances()``
        :param key: key used to get objects from list response
        :param **kwargs: keyword arguments passed to API list request
        """
        request = endpoint.list(**kwargs)

        instance_list = []
        while request is not None:
            response = request.execute()
            instance_list.extend(response.get(key, []))
            request = endpoint.list_next(  # pylint: disable=no-member
                previous_request=request, previous_response=response
            )
        return instance_list

    @staticmethod
    def _delete(
        name: str,
        endpoint: DiscoveryEndpoint,
        instance: Dict,
        key: str = "id",
        **kwargs,
    ):
        """
        Calls endpoint.delete(...).execute() to execute discovery API.

        :param name: name of object to delete, used for logging
        :param endpoint: An discovery API object for example ``self.client.instances()``
        :param instance: object to be deleted
        :param key: key to get instance name/id
        :param **kwargs: keyword arguments passed to API list request
        """
        singular_name = name[:-1] if name.endswith("s") else name
        logger.info(f"Deleting {singular_name}: {instance.get(key, 'unknown id')}")
        try:
            endpoint.delete(**kwargs).execute()
        except Exception as err:  # pylint: disable=broad-except
            logger.warning(f"Failed to delete {singular_name}: {err}")

    def _delete_all_in_location(self, location):
        raise NotImplementedError

    def _delete_in_all_locations(self, locations, object_name: str):
        for location in locations:
            logger.info(f"Deleting {object_name} in: {location}")
            try:
                self._delete_all_in_location(location=location)
            except HttpError as err:
                if int(err.resp["status"]) >= 500:
                    # Ignore server errors
                    pass
                elif "Unexpected location" in str(err):
                    pass
                else:
                    raise


class ComputeClient(BaseDiscoveryClient):
    endpoint = "compute"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._locations = []
        self._zones = []

    def _delete_all_in_location(self, location):
        raise NotImplementedError(
            "This function is not implemented for compute endpoint as there is more than one resource"
        )

    def _refresh_locations_and_zones(self, force: bool = False):
        if self._locations and not force:
            return

        locations_objects = self._iterate(
            endpoint=self.client.regions(), project=self.project_id
        )
        locations, zones = [], []
        for loc in locations_objects:
            locations.append(loc["name"])
            zones.extend([z.split("/")[-1] for z in loc.get("zones", [])])

        self._zones = zones
        self._locations = locations

    @property
    def locations(self):
        if not self._locations:
            self._refresh_locations_and_zones(force=True)
        return self._locations

    @property
    def zones(self) -> List[str]:
        if not self._zones:
            self._refresh_locations_and_zones(force=True)
        return self._zones

    def _delete_all(self, endpoint_name: str) -> None:
        """
        Iterates through all zones, lists object under given ``endpoint_name``
        and then deletes those objects.
        """
        for zone in self.zones:
            logger.info(f"Deleting compute {endpoint_name} in {zone}")
            endpoint = getattr(self.client, endpoint_name)()
            for obj in self._iterate(
                endpoint=endpoint, project=self.project_id, zone=zone
            ):
                if SKIP_LABEL not in obj.get("labels", {}) and self.is_stale(
                    obj["creationTimestamp"]
                ):
                    self._delete(
                        name=endpoint_name,
                        endpoint=endpoint,
                        instance=obj,
                        project=self.project_id,
                        zone=obj["zone"].split("/")[-1],
                        obj=obj["id"],
                    )

    def delete_all_disks(self) -> None:
        self._delete_all("disks")

    def delete_all_instances(self) -> None:
        self._delete_all("instances")


class DataprocClient(BaseDiscoveryClient):
    endpoint = "dataproc"

    def _delete_all_in_location(self, location: str):
        clusters = self._iterate(
            endpoint=self.client.projects().regions().clusters(),
            key="clusters",
            projectId=self.project_id,
            region=location,
        )
        for cluster in clusters:
            last_state_date = cluster["status"]["stateStartTime"]
            if SKIP_LABEL not in cluster.get("labels", []) and self.is_stale(
                last_state_date
            ):
                self._delete(
                    name="cluster",
                    endpoint=self.client.projects().regions().clusters(),
                    instance=cluster,
                    key="clusterName",
                    projectId=self.project_id,
                    region=location,
                    clusterName=cluster["clusterName"],
                )

    def delete_all_clusters(self, locations: List[str]):
        self._delete_in_all_locations(
            locations=locations, object_name="dataproc clusters"
        )


class ComposerClient(BaseDiscoveryClient):
    endpoint = "composer"

    def _delete_all_in_location(self, location: str) -> None:
        conn = self.client.projects().locations().environments()
        environments = self._iterate(
            endpoint=conn,
            key="environments",
            parent=f"projects/{self.project_id}/locations/{location}",
        )
        for env in environments:
            if SKIP_LABEL not in env.get("labels", {}) and self.is_stale(
                env["updateTime"]
            ):
                self._delete(name="composer", endpoint=conn, key="name", instance=env)

    def delete_all_environments(self, locations: List[str]) -> None:
        self._delete_in_all_locations(locations=locations, object_name="composers")


def run_cleaning(name, func, **kwargs):
    logger.warning(f"Attempting to clean {name}")
    try:
        func(**kwargs)
    except Exception:  # pylint: disable=broad-except
        logger.exception(f"Failed to clean {name}")


def main():
    warnings.filterwarnings(
        "ignore",
        message=".*Your application has authenticated using end user credentials.*",
    )

    logger.info("Starting the cleanup 🐈")

    # Discovery API
    credentials, project_id = google.auth.default()
    compute = ComputeClient(project_id=project_id, credentials=credentials)
    composer = ComposerClient(project_id=project_id, credentials=credentials)
    dataproc = DataprocClient(project_id=project_id, credentials=credentials)

    # Get locations and zones
    locations = compute.locations
    _ = compute.zones

    # Clean everything we can, mind that the order may have impact
    run_cleaning(
        "composer instances", composer.delete_all_environments, locations=locations
    )
    run_cleaning("dataproc clusters", dataproc.delete_all_clusters, locations=locations)
    run_cleaning("compute instances", compute.delete_all_instances)
    run_cleaning("compute disks", compute.delete_all_disks)

    logger.info("Done")


if __name__ == "__main__":
    main()
