import argparse
import time
from .SubjectiveDataSource import SubjectiveDataSource


class MockProgressDataSource(SubjectiveDataSource):
    def fetch(self):
        mocked_items = [f"item-{i}" for i in range(1, 201)]
        self.set_total_items(len(mocked_items))

        start = time.time()
        for item in mocked_items:
            # Simulate processing time per item.
            time.sleep(0.05)
            self.set_total_processing_time(time.time() - start)
            self.update({"item": item})

        self.set_fetch_completed(True)

    def get_icon(self):
        return "<svg></svg>"

    def get_connection_data(self):
        return {"connection_type": "Mock", "fields": []}


def _print_progress(data_source_name, total_items, processed_items, estimated_time):
    if total_items <= 0:
        remaining = "unknown"
    else:
        remaining = total_items - processed_items
    print(
        f"[{data_source_name}] processed={processed_items} "
        f"total={total_items} remaining={remaining} "
        f"eta={estimated_time}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Progress demo data source")
    parser.add_argument("--progress", action="store_true", help="Show a progress bar")
    args = parser.parse_args()

    ds = MockProgressDataSource(name="ProgressDemo", params={"progress": args.progress})
    ds.set_progress_callback(_print_progress)
    ds.fetch()
