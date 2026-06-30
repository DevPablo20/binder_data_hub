import sys

from src.pipelines.run_silver import main

if __name__ == "__main__":
    sys.argv = ["run_silver", "tiktok"]
    main()
