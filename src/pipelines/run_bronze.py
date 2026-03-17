import sys

from src.spark_session import get_spark_session
from src.transformers import (
    GoogleRawToBronzeTransformer,
)

_TRANSFORMERS = {
    "google": GoogleRawToBronzeTransformer,
}


def main() -> None:
    platform = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
    if not platform or platform not in _TRANSFORMERS:
        print(f"Uso: python -m src.pipelines.run_bronze <platform>")
        print(f"Plataformas: {', '.join(_TRANSFORMERS)}")
        sys.exit(1)

    spark = get_spark_session(app_name=f"bronze-{platform}")
    transformer = _TRANSFORMERS[platform](spark)
    transformer.run()
    spark.stop()
    print(f"Bronze concluído para {platform}.")


if __name__ == "__main__":
    main()
