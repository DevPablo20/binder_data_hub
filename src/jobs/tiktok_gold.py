import sys

from src.pipelines.run_gold import main

if __name__ == "__main__":
    sys.argv = ["run_gold", "tiktok"]
    main()
