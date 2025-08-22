import os, re, json, orjson
def split_chats(text: str):
lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
return lines


FORM_SPLITTERS = {
"poems": split_poetry,
"prose": split_prose,
"captions": split_prose,
"chats": split_chats,
"audiologs": split_prose,
}


TAGS_DEFAULT = {
"poems": {"form": "poem"},
"prose": {"form": "prose"},
"captions": {"form": "caption"},
"chats": {"form": "chat"},
"audiologs": {"form": "prose"},
}




def iter_files():
for sub in FORM_SPLITTERS.keys():
d = RAW / sub
if not d.exists():
continue
for p in sorted(d.rglob("*.txt")):
yield sub, p




def main():
OUT.parent.mkdir(parents=True, exist_ok=True)
n = 0
with open(OUT, "wb") as fout:
for sub, path in tqdm(list(iter_files())):
raw = path.read_text(encoding="utf-8", errors="ignore")
text = norm_text(raw)
chunks = FORM_SPLITTERS[sub](text)
for chunk in chunks:
if len(chunk) < 12: # skip tiny crumbs
continue
rec = {
"text": chunk,
"form": TAGS_DEFAULT[sub]["form"],
"source": f"{sub}/{path.name}",
"tags": []
}
fout.write(orjson.dumps(rec) + b"\n")
n += 1
print(f"wrote {n} segments → {OUT}")


if __name__ == "__main__":
main()