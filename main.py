from GSP import GSP
# ====== CLI INPUT + UNIWERSALNA WALIDACJA (wklej do main.py) ======
from typing import Callable, Optional, Type, TypeVar
import pandas as pd

T = TypeVar("T", int, float)

def ask_number(
    prompt: str,
    default: T,
    cast: Type[T],
    min_value: Optional[T] = None,
    max_value: Optional[T] = None,
    allow_empty: bool = True,
    predicate: Optional[Callable[[T], bool]] = None,
    predicate_msg: str = "Niepoprawna wartość.",
) -> T:
    while True:
        raw = input(f"{prompt} [domyślnie: {default}]: ").strip()

        if raw == "":
            if allow_empty:
                return default
            print("Wartość wymagana.")
            continue

        try:
            value = cast(raw)
        except ValueError:
            print(f"Podaj liczbę typu {cast.__name__}.")
            continue

        if min_value is not None and value < min_value:
            print(f"Wartość musi być >= {min_value}.")
            continue

        if max_value is not None and value > max_value:
            print(f"Wartość musi być <= {max_value}.")
            continue

        if predicate is not None and not predicate(value):
            print(predicate_msg)
            continue

        return value


def ask_string(
    prompt: str,
    default: str,
    allow_empty: bool = True,
    predicate: Optional[Callable[[str], bool]] = None,
    predicate_msg: str = "Niepoprawna wartość.",
) -> str:
    while True:
        raw = input(f"{prompt} [domyślnie: {default}]: ").strip()

        if raw == "":
            if allow_empty:
                return default
            print("Wartość wymagana.")
            continue

        if predicate is not None and not predicate(raw):
            print(predicate_msg)
            continue

        return raw


# ---- Domyślne wartości (ustaw swoje) ----
file = "Data/dane2.csv"
company = "company"
timestamp = "data"
price = "cena"

min_sup_pct = 5.0

# parametry w krokach data_int
seq_len = 24
seq_step = 1
min_gap = 0
max_gap = 1
win_size = None  # None = brak

# ile sekund odpowiada 1 krokowi data_int
time_bin_seconds = 3600  # 60=min, 3600=h, 86400=d

# ---- Wejście od użytkownika ----
file = ask_string("Podaj nazwę pliku CSV", file)

# Wczytaj nagłówek, żeby móc zwalidować kolumny zanim wczytasz całość
try:
    _header_df = pd.read_csv(file, nrows=5)
except Exception as e:
    raise SystemExit(f"Nie udało się wczytać pliku {file}: {e}")

print("\nKolumny w pliku:", _header_df.columns.tolist())

company = ask_string(
    "Kolumna spółki",
    company,
    predicate=lambda s: s in _header_df.columns,
    predicate_msg="Taka kolumna nie istnieje w pliku."
)

timestamp = ask_string(
    "Kolumna czasu",
    timestamp,
    predicate=lambda s: s in _header_df.columns,
    predicate_msg="Taka kolumna nie istnieje w pliku."
)

price = ask_string(
    "Kolumna ceny",
    price,
    predicate=lambda s: s in _header_df.columns,
    predicate_msg="Taka kolumna nie istnieje w pliku."
)

# Interwał (sekundy na 1 krok data_int)
time_bin_seconds = ask_number(
    "Interwał w sekundach (60=1min, 3600=1h, 86400=1d)",
    default=time_bin_seconds,
    cast=int,
    min_value=1
)

# min_sup w % (0..100)
min_sup_pct = ask_number(
    "Minimalny support w % (0..100), np. 10 (0 = pokaż wszystko)",
    default=min_sup_pct,
    cast=float,
    min_value=0.0,
    max_value=100.0
)

# parametry okien (w krokach data_int)
seq_len = ask_number(
    "Długość okna (seq_len) w krokach data_int",
    default=seq_len,
    cast=int,
    min_value=1
)

seq_step = ask_number(
    "Krok przesuwu okna (seq_step) w krokach data_int",
    default=seq_step,
    cast=int,
    min_value=1
)

# gapy (w krokach data_int)
min_gap = ask_number(
    "min_gap (>=0) w krokach data_int",
    default=min_gap,
    cast=int,
    min_value=0
)

max_gap = ask_number(
    "max_gap (>=min_gap) w krokach data_int",
    default=max_gap,
    cast=int,
    min_value=0,
    predicate=lambda x: x >= min_gap,
    predicate_msg="max_gap musi być >= min_gap."
)

# win_size: ENTER = brak (None) albo >=0
win_in = input(f"win_size (>=0) w krokach data_int, ENTER=brak [domyślnie: {win_size}]: ").strip()
if win_in == "":
    win_size = None
else:
    while True:
        try:
            w = int(win_in)
            if w < 0:
                print("win_size musi być >= 0 albo ENTER.")
                win_in = input("Podaj win_size ponownie: ").strip()
                continue
            win_size = w
            break
        except ValueError:
            print("Podaj liczbę całkowitą albo ENTER.")
            win_in = input("Podaj win_size ponownie: ").strip()

# ---- Wczytaj dane właściwe ----
df = pd.read_csv(file, usecols=[company, timestamp, price])

# Walidacja parsowania czasu (żeby złapać błędy od razu)
try:
    _ = pd.to_datetime(df[timestamp], errors="raise")
except Exception as e:
    raise SystemExit(f"Kolumna czasu '{timestamp}' nie daje się sparsować do daty/czasu: {e}")

print("\n===== PODSUMOWANIE PARAMETRÓW =====")
print("file:", file)
print("columns:", company, timestamp, price)
print("time_bin_seconds:", time_bin_seconds)
print("min_sup_pct:", min_sup_pct)
print("seq_len:", seq_len, "seq_step:", seq_step)
print("min_gap:", min_gap, "max_gap:", max_gap, "win_size:", win_size)
print("rows loaded:", len(df))
# ====== KONIEC BLOKU ======


gsp = GSP(
    transaction_data=df,
    company_col=company,
    time_col=timestamp,
    price_col=price,
    min_sup_pct=min_sup_pct,
    min_gap=min_gap,
    max_gap=max_gap,
    win_size=win_size,
    seq_len=seq_len,
    seq_step=seq_step,
    time_bin_seconds=time_bin_seconds
)

results = gsp.run()

print("\n================= BAZA OKIEN (DB) =================")
print("Liczba okien:", len(gsp.DB))

MAX_SHOW = len(gsp.DB)

for i, seq in enumerate(gsp.DB[:MAX_SHOW]):
    print(f"\n--- OKNO {i} ---")
    for t, itemset in seq:
        print(f"{t} -> {sorted(list(itemset))}")

print(f"\n... pokazano pierwsze {MAX_SHOW} okien ...")

if not results:
    print("Brak wzorców albo za mało danych po pct_change().")
else:
    print(f"\nDB size = {len(gsp.DB)} (liczba okien)")
    for k in sorted(results.keys()):
        print(f"\nWzorce długości {k}:")
        for pat, info in sorted(results[k].items(), key=lambda x: (-x[1]["support_pct"], x[0])):
            pretty = " -> ".join(["{" + ",".join(sorted(list(iset))) + "}" for iset in pat])
            print(f"{pretty}  count={info['count']}  support={info['support_pct']:.2f}%")







