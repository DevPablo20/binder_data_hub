import sys

from src.pipelines.run_bronze import main

if __name__ == "__main__":
    sys.argv = ["run_bronze", "tiktok"]
    main()
