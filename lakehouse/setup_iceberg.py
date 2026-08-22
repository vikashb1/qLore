from pyiceberg.catalog import load_catalog


CATALOG_URI = "http://localhost:8181"

WAREHOUSE = "s3://warehouse"


def get_catalog():
    """
    Connect to the qLore Iceberg REST catalog.

    MinIO acts as our local S3-compatible object store.
    """

    catalog = load_catalog(
        "qlore",
        type="rest",
        uri=CATALOG_URI,
        warehouse=WAREHOUSE,

        # MinIO / S3 configuration
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
        },
    )

    return catalog


def create_namespaces(catalog):

    namespaces = [
        "bronze",
        "silver",
        "gold",
    ]

    existing_namespaces = {
        ".".join(namespace)
        for namespace in catalog.list_namespaces()
    }

    print("\nExisting namespaces:")
    print(existing_namespaces)

    for namespace in namespaces:

        if namespace in existing_namespaces:

            print(
                f"Namespace already exists: "
                f"{namespace}"
            )

        else:

            catalog.create_namespace(
                namespace
            )

            print(
                f"Created namespace: "
                f"{namespace}"
            )


def main():

    print("\nqLore Iceberg Setup")
    print("=" * 70)

    catalog = get_catalog()

    create_namespaces(
        catalog
    )

    print("\nFinal namespaces:")

    for namespace in catalog.list_namespaces():

        print(
            " - "
            + ".".join(namespace)
        )

    print("\nIceberg namespace setup complete.")


if __name__ == "__main__":
    main()