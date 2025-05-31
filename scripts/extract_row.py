# find_line_21497.py

import itertools

CSV_FILE = '/Users/azim/CLionProjects/FastLanes_Data/NextiaJD/tables/bitcoin_reddit_all/bitcoin_reddit_all.csv'
TARGET_ID = '21497|'  # We look for lines that begin with "21497|"

def find_id_lines(path, target_id, context=2):
    """
    Scans `path` line by line and prints any lines that start with `target_id`.
    Also prints `context` lines before and after for easy inspection.
    """
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        # We’ll keep a rolling buffer of the previous `context` lines:
        prev_lines = []
        for lineno, raw_line in enumerate(f, start=1):
            if raw_line.startswith(target_id):
                print(f"\n--- Found line starting with '{target_id}' at raw line {lineno} ---")
                # Print the `context` lines before:
                for plineno, p_text in enumerate(prev_lines, start=lineno - len(prev_lines)):
                    print(f"{plineno:>6}: {p_text.rstrip()!r}")
                # Print the matching line itself:
                print(f"{lineno:>6}: {raw_line.rstrip()!r}    <-- this line")
                # Now print the next `context` lines (lookahead):
                for offset, next_line in zip(range(1, context+1), itertools.islice(f, context)):
                    print(f"{lineno + offset:>6}: {next_line.rstrip()!r}")
                print("-" * 60)
                # If you only expect exactly one match, you can break here.
                # Otherwise, this will continue searching for any other occurrences.
            # Slide the buffer of the last `context` lines:
            prev_lines.append(raw_line)
            if len(prev_lines) > context:
                prev_lines.pop(0)

if __name__ == '__main__':
    find_id_lines(CSV_FILE, TARGET_ID, context=2)
