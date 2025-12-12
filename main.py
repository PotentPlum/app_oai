from src.app_service import AppService
from src.ui.dashboard import Dashboard


def main() -> None:
    service = AppService()
    root = Dashboard(service)
    root.mainloop()


if __name__ == "__main__":
    main()
