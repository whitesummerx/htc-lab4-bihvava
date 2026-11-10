import os
import signal
import sys

import uvicorn


def main() -> None:
    # Після коректної зупинки uvicorn повторно надсилає собі перехоплений сигнал,
    # відновивши попередній обробник. Якщо він типовий, процес гине від SIGTERM
    # і docker показує код 143. Тому заздалегідь ставимо обробник, який сигнал ігнорує:
    # uvicorn на час роботи підміняє його своїм, а після зупинки повертає цей.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    config = uvicorn.Config(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        # має бути меншим за stop_grace_period (Compose, 30 с) і за terminationGracePeriodSeconds
        # мінус пауза preStop (Kubernetes); у Kubernetes задається змінною з ConfigMap
        timeout_graceful_shutdown=int(os.environ.get("GRACEFUL_SHUTDOWN_SECONDS", "25")),
    )
    uvicorn.Server(config).run()
    sys.exit(0)


if __name__ == "__main__":
    main()
