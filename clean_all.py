import datetime
import warnings
from typing import Any, Dict, List

import google
import google.auth
from google.auth.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger

SKIP_LABEL = "please-do-not-kill-me"

DiscoveryEndpoint = Any


class BaseDiscoveryClient:
    endpoint = None
    version = "v1"

    def __init__(self, project_id: str, credentials: Credentials):
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
        endpoint: DiscoveryEndpoint,
        payload: Dict,
        key: str = "items",
    ) -> List[Dict]:
        """
        Iterates through endpoint.list(...).execute() discovery API endpoint

        :param endpoint: An discovery API object for example ``self.client.instances()``
        :param key: key used to get objects from list response
        :param payload: keyword arguments passed to API list request
        """
        request = endpoint.list(**payload)

        instance_list = []
        while request is not None:
            response = request.execute()

            instance_list.extend(response.get(key, []))
            try:
                request = endpoint.list_next(  # pylint: disable=no-member
                    previous_request=request, previous_response=response
                )
            except AttributeError:
                # In some cases API may return all resources in list request
                break
        return instance_list

    @staticmethod
    def _singular_name(name: str) -> str:
        return name[:-1] if name.endswith("s") else name

    def _delete(
        self,
        resource_name: str,
        resource_id: str,
        endpoint: DiscoveryEndpoint,
        payload: Dict,
    ):
        """
        Calls endpoint.delete(...).execute() to execute discovery API.

        :param resource_name: name of object to delete, used for logging, for example ``clusters``
        :param resource_id: id fo the resource to delete, used for logging
        :param endpoint: An discovery API object for example ``self.client.instances()``
        :param payload: keyword arguments passed to API delete request
        """
        singular_name = self._singular_name(resource_name)
        logger.info(f"Deleting {singular_name}: {resource_id}")
        try:
            endpoint.delete(**payload).execute()
        except Exception as err:  # pylint: disable=broad-except
            logger.warning(f"Failed to delete {singular_name}: {err}")

    def _delete_all_in_location(self, location):
        raise NotImplementedError

    def _delete_in_all_locations(self, locations, object_name: str):
        for location in locations:
            logger.debug(f"Deleting {object_name} in: {location}")
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
            endpoint=self.client.regions(), payload={"project": self.project_id}
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
            logger.debug(f"Deleting compute {endpoint_name} in {zone}")
            endpoint = getattr(self.client, endpoint_name)()
            for obj in self._iterate(
                endpoint=endpoint, payload={"project": self.project_id, "zone": zone}
            ):
                is_not_labeled = SKIP_LABEL not in obj.get("labels", {})
                is_stale = self.is_stale(obj["creationTimestamp"])
                # In case of disk we check if it is used by anything else
                # in case of other resources we return True
                has_no_users = not bool(obj.get("users"))

                if is_not_labeled and is_stale and has_no_users:
                    self._delete(
                        resource_name=endpoint_name,
                        resource_id=obj["id"],
                        endpoint=endpoint,
                        payload={
                            "project": self.project_id,
                            "zone": obj["zone"].split("/")[-1],
                            self._singular_name(endpoint_name): obj["id"],
                        },
                    )

    def delete_all_disks(self) -> None:
        self._delete_all("disks")

    def delete_all_instances(self) -> None:
        self._delete_all("instances")


class GKEClient(BaseDiscoveryClient):
    endpoint = "container"

    def _delete_all_in_location(self, location: str):
        raise NotImplementedError(
            "GKEClient is able to list all clusters in one request."
        )

    def delete_all_clusters(self):
        logger.debug("Deleting GKE clusters in ALL locations")
        endpoint = self.client.projects().locations().clusters()
        list_response = endpoint.list(
            parent=f"projects/{self.project_id}/locations/-"
        ).execute()

        if "clusters" not in list_response:
            logger.error("No `clusters` in in GKE api.")
            return

        for cluster in list_response["clusters"]:
            if SKIP_LABEL not in cluster.get("resourceLabels", {}) and self.is_stale(
                cluster["createTime"]
            ):
                cluster_name = cluster["name"]
                zone = cluster["zone"]
                self._delete(
                    resource_name="cluster",
                    resource_id=cluster_name,
                    endpoint=self.client.projects().locations().clusters(),
                    payload={
                        "name": f"projects/{self.project_id}/locations/{zone}/clusters/{cluster_name}"
                    },
                )


class DataprocClient(BaseDiscoveryClient):
    endpoint = "dataproc"

    def _delete_all_in_location(self, location: str):
        clusters = self._iterate(
            endpoint=self.client.projects().regions().clusters(),
            key="clusters",
            payload={
                "projectId": self.project_id,
                "region": location,
            },
        )
        for cluster in clusters:
            last_state_date = cluster["status"]["stateStartTime"]
            if SKIP_LABEL not in cluster.get("labels", []) and self.is_stale(
                last_state_date
            ):
                self._delete(
                    resource_name="cluster",
                    resource_id=cluster["clusterName"],
                    endpoint=self.client.projects().regions().clusters(),
                    payload={
                        "projectId": self.project_id,
                        "region": location,
                        "clusterName": cluster["clusterName"],
                    },
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
            payload={"parent": f"projects/{self.project_id}/locations/{location}"},
        )
        for env in environments:
            if SKIP_LABEL not in env.get("labels", {}) and self.is_stale(
                env["updateTime"]
            ):
                self._delete(
                    resource_name="composer",
                    resource_id=env["name"].split("/")[-1],
                    endpoint=conn,
                    payload={"name": env["name"]},
                )

    def delete_all_environments(self, locations: List[str]) -> None:
        self._delete_in_all_locations(locations=locations, object_name="composers")


class MemorystoreRedisClient(BaseDiscoveryClient):
    endpoint = "redis"

    def _delete_all_in_location(self, location: str):
        instances = self._iterate(
            endpoint=self.client.projects().locations().instances(),
            key="instances",
            payload={"parent": f"projects/{self.project_id}/locations/{location}"},
        )

        for instance in instances:
            logger.info(instance)
            create_date = instance["createTime"]
            if SKIP_LABEL not in instance.get("labels", []) and self.is_stale(
                create_date
            ):
                self._delete(
                    resource_name="instance",
                    resource_id=instance["name"],
                    endpoint=self.client.projects().locations().instances(),
                    payload={
                        "name": instance["name"],
                    },
                )

    def delete_all_instances(self, locations: List[str]):
        self._delete_in_all_locations(
            locations=locations, object_name="memorystore redis instances"
        )


def run_cleaning(name, func, **kwargs):
    logger.warning(f"Attempting to clean {name}")
    try:
        func(**kwargs)
        logger.info(f"Cleaning of {name} done")
    except Exception:  # pylint: disable=broad-except
        logger.exception(f"Failed to clean {name}")


def delete_resources():
    warnings.filterwarnings(
        "ignore",
        message=".*Your application has authenticated using end user credentials.*",
    )

    logger.info("Starting the cleanup 🐈")

    # Discovery API
    credentials, project_id = google.auth.default()
    composer = ComposerClient(project_id=project_id, credentials=credentials)
    gke = GKEClient(project_id=project_id, credentials=credentials)
    dataproc = DataprocClient(project_id=project_id, credentials=credentials)
    compute = ComputeClient(project_id=project_id, credentials=credentials)
    memorystore_redis = MemorystoreRedisClient(
        project_id=project_id, credentials=credentials
    )

    # Get locations and zones
    locations = compute.locations
    _ = compute.zones

    # Clean everything we can, mind that the order may have impact
    run_cleaning(
        "composer instances", composer.delete_all_environments, locations=locations
    )
    run_cleaning("GKE clusters", gke.delete_all_clusters)
    run_cleaning("dataproc clusters", dataproc.delete_all_clusters, locations=locations)
    run_cleaning("compute instances", compute.delete_all_instances)
    run_cleaning("compute disks", compute.delete_all_disks)
    run_cleaning(
        "memorystore redis instances",
        memorystore_redis.delete_all_instances,
        locations=locations,
    )

    logger.info("Done")
