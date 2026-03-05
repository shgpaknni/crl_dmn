# crl_dmn

BFS domain crawler + ranker.

## Exact flow implemented
For one crawl session:
1. Pick one seed domain (Rank 0).
2. BFS crawl up to depth 3.
3. For each discovered domain:
   - If in branded dataset: skip (store in `skipped_branded.txt`, do not rank, do not crawl).
   - If not in branded dataset: store in rank file by depth (`rank1/2/3`), and crawl only if depth `< 3`.
4. Extract/store only root (apex) domains, never subdomains.

## Output (5 files per seed)
Inside `<output>/<seed_as_folder>/`:
- `rank0_seed.txt`
- `rank1.txt`
- `rank2.txt`
- `rank3.txt`
- `skipped_branded.txt`

## Run (single seed; recommended)
```bash
python domain_bfs_ranker.py --branded branded_domains.txt --seed nike.com --output out
```

## Run (batch mode)
```bash
python domain_bfs_ranker.py --branded branded_domains.txt --seeds my_seed_list.txt --output out
```
If `--seeds` is omitted, branded file is also used as seed list.

## Performance options (for very large files)
```bash
python domain_bfs_ranker.py \
  --branded branded_domains.txt \
  --seed nike.com \
  --output out \
  --workers 64 \
  --timeout 6 \
  --max-read-bytes 2000000 \
  --max-nodes-per-depth 0
```

- `--workers`: concurrent fetches per BFS level.
- `--max-nodes-per-depth`: safety cap for breadth explosion (`0` = unlimited).
- `--max-read-bytes`: page-read cap per request.
- `--max-seeds`: limits seeds in batch mode.

## Windows PowerShell example
```powershell
python .\domain_bfs_ranker.py --branded ".\branded_domains.txt" --seed "nike.com" --output ".\out"
```

## Optional accuracy boost for apex extraction
Install `tldextract` for better public-suffix handling:
```bash
pip install tldextract
```
If not installed, script uses built-in fallback heuristics.
