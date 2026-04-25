"""quant-report CLI entry point."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if __name__ == "__main__":
    from agent4quant.report import main
    main()
