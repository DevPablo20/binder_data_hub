import sys

from src.spark_session import get_spark_session
from src.transformers import TikTokBronzeToSilverTransformer

_TRANSFORMERS = {
    "tiktok": TikTokBronzeToSilverTransformer,
}


def main() -> None:
    platform = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
    if not platform or platform not in _TRANSFORMERS:
        print("Uso: python -m src.pipelines.run_silver <platform>")
        print(f"Plataformas: {', '.join(_TRANSFORMERS)}")
        sys.exit(1)

    spark = get_spark_session(app_name=f"silver-{platform}")
    transformer = _TRANSFORMERS[platform](spark)
    transformer.run()
    spark.stop()
    print(f"Silver concluído para {platform}.")


if __name__ == "__main__":
    main()
