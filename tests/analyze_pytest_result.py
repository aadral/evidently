#!/usr/bin/env python3
import json
import sys


if __name__ == "__main__":
    pytest_report_fpath = "pytest_result.jsonl" if len(sys.argv) < 2 else sys.argv[1]

    current_nodeid = None

    for line in open(pytest_report_fpath):
        jsonline = json.loads(line.rstrip())
        if "nodeid" in jsonline:
            # TODO: extract Metric name from nodeid
            current_nodeid = jsonline["nodeid"]

        # TODO: collect and report all warnings for one nodeid once
        if "PandasAPIOnSparkAdviceWarning" == jsonline.get("category"):
            print(f"{current_nodeid} emits PandasAPIOnSparkAdviceWarning: {jsonline['message']}")
