import json
import sys
from pathlib import Path
import pandas as pd

OUT_DIR = Path("/opt/airflow/data/transformed") 

def transform(input_file: str):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    hits = data.get("hits", [])
    df = pd.json_normalize(hits)

    if "type" not in df.columns:
        def _infer_type(tags):
            if isinstance(tags, list) and "story" in tags:
                return "story"
            return "comment"
        df["type"] = df.get("_tags", []).apply(_infer_type if "_tags" in df.columns else (lambda _: "comment"))

    counts = df.groupby("type", dropna=False).size().reset_index(name="count")
    counts_path = OUT_DIR / "count_by_type.csv"
    counts.to_csv(counts_path, index=False)

    stories = df[df["type"] == "story"].copy()

    stories["points"] = pd.to_numeric(stories.get("points", 0), errors="coerce").fillna(0).astype(int)
    stories["num_comments"] = pd.to_numeric(stories.get("num_comments", 0), errors="coerce").fillna(0).astype(int)

    id_col = "story_id" if "story_id" in stories.columns else ("objectID" if "objectID" in stories.columns else None)
    if id_col is None:
        stories["story_id"] = range(1, len(stories) + 1)
        id_col = "story_id"

    cols = [id_col, "title", "points", "num_comments"]
    existing = [c for c in cols if c in stories.columns]
    story_metrics = stories[existing].drop_duplicates()
    if id_col != "story_id":
        story_metrics = story_metrics.rename(columns={id_col: "story_id"})

    metrics_path = OUT_DIR / "story_metrics.csv"
    story_metrics.to_csv(metrics_path, index=False)

    print(f"✅ Wrote {counts_path}")
    print(f"✅ Wrote {metrics_path}")

if __name__ == "__main__":
    transform(sys.argv[1])