from clean_all import delete_resources


def delete_gcp_resources(*args, **kwargs):  # pylint: disable=unused-argument
    """Simple function for triggering via cloud functions"""
    delete_resources()
    return "SUCCESS"


if __name__ == "__main__":
    delete_gcp_resources()
