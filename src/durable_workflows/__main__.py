import uvicorn

from durable_workflows.config import Settings


def main() -> None:
    settings = Settings.from_env()
    uvicorn.run(
        "durable_workflows.app:app",
        host=settings.host,
        port=settings.port,
        reload=False,
    )


if __name__ == "__main__":
    main()
