import pandas as pd
from typing import List, Set, Tuple, Dict, FrozenSet, Optional
from tree import count_support_db_int

Pattern = Tuple[FrozenSet[str], ...]
TimedSequenceInt = List[Tuple[int, Set[str]]]


def discretize_return(x: float) -> str:
    if -0.001 <= x <= 0.001:
        return "0"
    elif x > 0.001:
        return "1"
    else:
        return "-1"


class GSP:
    def __init__(
        self,
        transaction_data: pd.DataFrame,
        company_col: str,
        time_col: str,
        price_col: str,
        min_sup_pct: float,          # support w %
        min_gap: int,                # w jednostkach data_int
        max_gap: int,                # w jednostkach data_int
        win_size: Optional[int],     # w jednostkach data_int (None = brak)
        seq_len: int,                # długość okna w jednostkach data_int
        seq_step: int,               # krok przesuwu okna w jednostkach data_int
        time_bin_seconds: int        # ile sekund odpowiada 1 krokowi data_int
    ):
        self.df = transaction_data.copy()
        self.company_col = company_col
        self.time_col = time_col
        self.price_col = price_col

        self.min_sup_pct = float(min_sup_pct)
        self.min_gap = int(min_gap)
        self.max_gap = int(max_gap)
        self.win_size = None if win_size is None else int(win_size)

        self.seq_len = int(seq_len)
        self.seq_step = int(seq_step)

        self.time_bin_seconds = int(time_bin_seconds)

        self.return_val_col = "stopa zwrotu"
        self.return_type_col = "typ stopy zwrotu"
        self.item_col = "item"
        self.t_int_col = "data_int"

        self.DB: List[TimedSequenceInt] = []

    # =========================
    # Pretty print helpers
    # =========================
    @staticmethod
    def pattern_to_str(p: Pattern) -> str:
        return "<" + ",".join(
            "{" + ",".join(sorted(list(iset))) + "}"
            for iset in p
        ) + ">"

    @staticmethod
    def print_pattern_list(title: str, patterns: List[Pattern]) -> None:
        print(f"\n===== {title} =====")
        if not patterns:
            print("(pusto)")
            return
        for p in patterns:
            print(GSP.pattern_to_str(p))

    @staticmethod
    def print_pattern_support(title: str, patterns: List[Pattern], sup_map: Dict[Pattern, int], db_size: int) -> None:
        print(f"\n===== {title} =====")
        if not patterns:
            print("(pusto)")
            return
        for p in patterns:
            sup = sup_map.get(p, 0)
            pct = (sup / db_size) * 100.0 if db_size else 0.0
            print(f"{GSP.pattern_to_str(p)}  count={sup}  support={pct:.2f}%")

    # =========================
    # DB building (Model B)
    # =========================
    def prepare_db(self) -> List[TimedSequenceInt]:
        df = self.df

        df[self.time_col] = pd.to_datetime(df[self.time_col])
        df = df.sort_values([self.company_col, self.time_col])

        # stopa zwrotu per spółka
        df[self.return_val_col] = df.groupby(self.company_col)[self.price_col].pct_change()
        # nie tracimy pierwszego rekordu spółki
        df[self.return_val_col] = df[self.return_val_col].fillna(0.0)

        # dyskretyzacja -1/0/1
        df[self.return_type_col] = df[self.return_val_col].apply(discretize_return)

        # item: spółka + typ
        df[self.item_col] = df[self.company_col].astype(str) + "_" + df[self.return_type_col].astype(str)

        # data_int: krok czasu od początku danych (w zadanym interwale)
        t0 = df[self.time_col].min()
        df[self.t_int_col] = ((df[self.time_col] - t0).dt.total_seconds() // self.time_bin_seconds).astype(int)

        # agregacja model B: data_int -> itemset
        t_itemsets = (
            df.sort_values(self.t_int_col)
              .groupby(self.t_int_col)[self.item_col]
              .apply(lambda s: set(s))
              .reset_index()
              .sort_values(self.t_int_col)
              .reset_index(drop=True)
        )

        times_int = t_itemsets[self.t_int_col].tolist()
        itemsets = t_itemsets[self.item_col].tolist()

        DB: List[TimedSequenceInt] = []
        if not times_int:
            self.DB = []
            self.df = df
            return []

        # okna startują tylko na realnych data_int
        i = 0
        while i < len(times_int):
            start_t = times_int[i]
            end_t = start_t + self.seq_len  # [start_t, end_t)

            seq: TimedSequenceInt = []
            j = i
            while j < len(times_int) and times_int[j] < end_t:
                seq.append((times_int[j], itemsets[j]))
                j += 1

            if seq:
                DB.append(seq)

            target = start_t + self.seq_step
            while i < len(times_int) and times_int[i] < target:
                i += 1

        self.DB = DB
        self.df = df
        return DB

    # =========================
    # FULL GSP join (S-step + I-step)
    # =========================
    @staticmethod
    def join_step(F_prev: List[Pattern]) -> List[Pattern]:
        cands: List[Pattern] = []
        F_prev_sorted = sorted(F_prev)

        def max_item(iset: FrozenSet[str]) -> str:
            return max(iset)

        for a in F_prev_sorted:
            for b in F_prev_sorted:
                if a == b:
                    continue

                # ---- S-step: doklej nowy itemset na koniec ----
                if a[1:] == b[:-1]:
                    cands.append(a + (b[-1],))

                # ---- I-step: doklej element do ostatniego itemsetu ----
                if a[:-1] == b[:-1]:
                    last_a = a[-1]
                    last_b = b[-1]

                    # doklejamy jeden element (klasyczny I-step)
                    if len(last_b) == 1:
                        x = next(iter(last_b))

                        # reguła porządku => x większe od max(last_a)
                        if x not in last_a and x > max_item(last_a):
                            merged = frozenset(set(last_a) | {x})
                            cands.append(a[:-1] + (merged,))

        return list(dict.fromkeys(cands))

    # =========================
    # FULL prune (also I-step pruning)
    # =========================
    @staticmethod
    def prune_step(Ck: List[Pattern], F_prev_set: set) -> List[Pattern]:
        pruned: List[Pattern] = []

        for c in Ck:
            ok = True

            # (A) usuwanie CAŁEGO itemsetu ma sens tylko, gdy itemset jest jednoelementowy
            for i, iset in enumerate(c):
                if len(iset) == 1:
                    sub = c[:i] + c[i + 1:]
                    if sub not in F_prev_set:
                        ok = False
                        break
            if not ok:
                continue

            # (B) zawsze sprawdzamy podwzorce przez usunięcie 1 elementu z itemsetu (gdy >1)
            for i, iset in enumerate(c):
                if len(iset) <= 1:
                    continue
                for x in iset:
                    smaller_iset = frozenset(set(iset) - {x})
                    sub = c[:i] + (smaller_iset,) + c[i + 1:]
                    if sub not in F_prev_set:
                        ok = False
                        break
                if not ok:
                    break

            if ok:
                pruned.append(c)

        return pruned

    # =========================
    # RUN (with debug prints per iteration)
    # =========================
    def run(self):
        if not self.DB:
            self.prepare_db()

        DB = self.DB
        db_size = len(DB)
        if db_size == 0:
            print("WARNING: DB is empty.")
            return {}

        # minsup % -> minsup count (0% -> 0)
        raw = (self.min_sup_pct / 100.0) * db_size
        if self.min_sup_pct <= 0:
            min_sup_count = 0
        else:
            min_sup_count = int(raw)
            if raw > min_sup_count:
                min_sup_count += 1

        print("\n===== PARAMS =====")
        print("DB size:", db_size)
        print("min_sup_pct:", self.min_sup_pct)
        print("min_sup_count:", min_sup_count)
        print("min_gap:", self.min_gap, "max_gap:", self.max_gap, "win_size:", self.win_size)
        print("seq_len:", self.seq_len, "seq_step:", self.seq_step)

        # ===== F1 scan =====
        item_support: Dict[str, int] = {}
        for seq in DB:
            seen = set()
            for _, X in seq:
                seen |= X
            for item in seen:
                item_support[item] = item_support.get(item, 0) + 1

        F1: List[Pattern] = [
            (frozenset([item]),)
            for item, sup in item_support.items()
            if sup >= min_sup_count
        ]

        # results
        results: Dict[int, Dict[Pattern, Dict[str, float]]] = {
            1: {
                p: {
                    "count": float(item_support[next(iter(p[0]))]),
                    "support_pct": (item_support[next(iter(p[0]))] / db_size) * 100.0
                }
                for p in F1
            }
        }

        # debug prints: F1 + support
        self.print_pattern_list("F1 (frequent length 1)", F1)
        print("\n===== F1 support =====")
        for p in F1:
            item = next(iter(p[0]))
            cnt = item_support[item]
            pct = (cnt / db_size) * 100.0
            print(f"{GSP.pattern_to_str(p)}  count={cnt}  support={pct:.2f}%")

        # ===== Iteracje k>=2 =====
        F_prev = F1
        k = 2
        while F_prev:
            # JOIN
            Ck = GSP.join_step(F_prev)
            self.print_pattern_list(f"C{k} po JOIN", Ck)

            # PRUNE
            Ck = GSP.prune_step(Ck, set(F_prev))
            self.print_pattern_list(f"C{k} po PRUNE", Ck)

            if not Ck:
                break

            # SUPPORT
            sup_map = count_support_db_int(
                DB=DB,
                patterns=Ck,
                min_gap=self.min_gap,
                max_gap=self.max_gap,
                win_size=self.win_size
            )

            Fk = [p for p, sup in sup_map.items() if sup >= min_sup_count]
            if not Fk:
                break

            # debug: frequent with support
            self.print_pattern_support(f"F{k} (frequent) + support", Fk, sup_map, db_size)

            results[k] = {
                p: {
                    "count": float(sup_map[p]),
                    "support_pct": (sup_map[p] / db_size) * 100.0
                }
                for p in Fk
            }

            F_prev = Fk
            k += 1

        return results



#
# class GSP:
#     def __init__(self, transaction_data_test, company, timestamp, price, min_sup, max_gap, min_gap, win_size):
#         self.company = company
#         self.timestamp = timestamp
#         self.price = price
#         self.rate_of_return_value = 'stopa zwrotu'
#         self.rate_of_return_type = 'typ stopy zwrotu'
#         self.min_sup_val = min_sup
#         self.id = 'sekwencja'
#         self.support_value = 'wskaznik wsparcia'
#         self.transaction_data_test = transaction_data_test
#         self.max_gap = max_gap
#         self.min_gap = min_gap
#         self.win_size = win_size
#
#     def generate_shorter_sequences(self, input_string):
#         # Remove the angle brackets and split the string into tuples
#         input_string = input_string[1:-1]
#         sequences = input_string.split("),(")
#         sequences = [tuple(map(int, seq.replace("(", "").replace(")", "").split(','))) for seq in sequences]
#
#         shorter_sequences = []
#
#         for i, seq in enumerate(sequences):
#             if len(seq) > 1:
#                 # Remove the first element
#                 shorter_sequences.append(sequences[:i] + [seq[1:]] + sequences[i+1:])
#                 # Remove the last element
#                 shorter_sequences.append(sequences[:i] + [seq[:-1]] + sequences[i+1:])
#                 # Remove each element one by one (if length > 2)
#                 for j in range(1, len(seq) - 1):
#                     shorter_sequences.append(sequences[:i] + [seq[:j] + seq[j+1:]] + sequences[i+1:])
#             elif i == 0 or i == len(sequences)-1:
#                 shorter_sequences.append(sequences[:i] + sequences[i+1:])
#
#         # Format the output as a string with angle brackets
#         formatted_output = [f"<{','.join(['(' + ','.join(map(str, s)) + ')' for s in seq])}>" for seq in shorter_sequences]
#         return formatted_output
#
#     def pars_sequences(self, s1, s2):
#         new_row = pd.DataFrame(columns=[self.id])
#         s1p = re.findall(r'\d+', s1)
#         s1p = s1p[1:]
#         s2p = re.findall(r'\d+', s2)
#         s2p = s2p[:-1]
#         if s1p == s2p:
#             tuples = s2.strip('<>').split(',')
#             if tuples[len(tuples) - 1][0] == '(':
#                 value = '<' + s1.strip('<>') + ',' + tuples[len(tuples) - 1] + '>'
#                 new_row = pd.DataFrame({self.id: [value]})
#             elif tuples[len(tuples) - 1][0].isdigit():
#                 value = s1.strip('<>')
#                 value = value[:-1]
#                 value = '<' + value + tuples[len(tuples) - 1] + '>'
#                 new_row = pd.DataFrame({self.id: [value]})
#         return new_row
#
#     def return_type(self, x):
#         if -0.001 <= x <= 0.001:
#             return "0"
#         elif x > 0.001:
#             return "1"
#         elif x < 0.001:
#             return "-1"
#         else:
#             return "-2"
#
# # #Dane źródłowe
# # transaction_data_test = pd.read_csv('BAMXF/BAMXF2020.csv', usecols=['seqnum','timestamp','mktcenter','price','shares','salescondition','canceled','dottchar','issuechar','msgseqnum','originalmsgseqnum','submkt'])
# # with pd.option_context('display.max_rows', 10, 'display.max_columns', None, 'display.width', 1000):
# #     print(transaction_data_test)
#
# #definicja_kolumn
#     def calculate(self):
#
#         #dane testowe
#         #self.transaction_data_test = pd.read_csv('new_transactions.csv', usecols=[self.company, self.timestamp, self.price])
#         self.transaction_data_test[self.timestamp] = pd.to_datetime(self.transaction_data_test[self.timestamp])
#         self.transaction_data_test = self.transaction_data_test.sort_values(by=[self.company, self.timestamp])
#         #liczymy wzrosty i spadki
#         self.transaction_data_test[self.rate_of_return_value] = self.transaction_data_test.groupby(self.company)[self.price].pct_change()
#         self.transaction_data_test = self.transaction_data_test.dropna()
#
#         #sortujemy dane, do grupowania w koszyki
#         self.transaction_data_test = self.transaction_data_test.sort_values(by=[self.timestamp, self.company])
#         self.transaction_data_test = self.transaction_data_test.head(50)
#         with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
#             print(self.transaction_data_test)
#
#
#         #Zamieniamy stopy zwrotu na -1, 0 ,1 -> spadek, stała, wzrost
#         self.transaction_data_test[self.rate_of_return_type] = self.transaction_data_test[self.rate_of_return_value].apply(self.return_type)
#         #with pd.option_context('display.max_rows', 10, 'display.max_columns', None, 'display.width', 1000):
#             #print(self.transaction_data_test)
#
#         #Tworzmy elementy do wzorca sekwencji
#         self.transaction_data_test["item"] = self.transaction_data_test["company"] + "_" + self.transaction_data_test["typ stopy zwrotu"]
#
#         # Dyskretyzacja czasu na wartości liczbowe
#         self.transaction_data_test["data_int"] = (
#             (self.transaction_data_test["data"] - self.transaction_data_test["data"].min())
#             .dt.total_seconds()
#             .div(3600)
#             .astype(int)
#         )
#         #Utworzenie elementów sekwencji
#         sequence_elements = (
#             self.transaction_data_test
#             .sort_values("data_int")
#             .groupby("data_int")["item"]
#             .apply(set)  # używamy set zamiast list
#             .to_dict()
#         )
#
#         #Utworzenie bazy sekwencji
#         #seq_db = 1
#         with pd.option_context('display.max_rows', 10, 'display.max_columns', None, 'display.width', 1000):
#             print(self.transaction_data_test)
#             print(sequence_elements)
#         #Obliczanie min_sup dla każego company, rate_of_return_value
#         min_sup_table = self.transaction_data_test.groupby([self.company, self.rate_of_return_type]).size()
#         min_sup_table = min_sup_table.to_frame()
#         min_sup_table.rename(columns={min_sup_table.columns[0]: self.support_value}, inplace=True)
#
#         #Odfiltrowanie ponieżej min_sup:
#         min_sup_table = min_sup_table[min_sup_table[self.support_value] >= self.min_sup_val]
#         # Usuwanie kolumny min_sup_val z tabeli
#         min_sup_table.drop(columns=[self.support_value], inplace=True)
#
#         #Odfiltrowanie danych z transaction_data_test na podstawie min_sup_table przez join
#         self.transaction_data_test = self.transaction_data_test.merge(min_sup_table, on=[self.company, self.rate_of_return_type])
#
#         #Odwzorowanie sekwencji -> zmiana kodu na klucz 'A-3' -> 1
#         self.transaction_data_test[self.id] = self.transaction_data_test.groupby([self.company, self.rate_of_return_type]).ngroup()
#
#
#         #SORTOWANIE PRZED ALGORYTMEM BARDZO WAŻNY KROK!!!
#         self.transaction_data_test = self.transaction_data_test.sort_values(by=[self.timestamp, self.company])
#         self.transaction_data_test = self.transaction_data_test.reset_index(drop=True).reset_index()
#         with pd.option_context('display.max_rows', 15, 'display.max_columns', None, 'display.width', 1000):
#             print(self.transaction_data_test)
#             print("")
#
#         transaction_count = self.transaction_data_test.__len__()
#         max_id = self.transaction_data_test[self.id].max()+1
#
#         #Lista sekwencji częstych
#         self.transaction_data_test[self.support_value] = self.transaction_data_test['count'] = self.transaction_data_test.groupby([self.id]).transform('size')
#         LSK = self.transaction_data_test.drop_duplicates(subset=[self.id])
#
#         with pd.option_context('display.max_rows', 10, 'display.max_columns', None, 'display.width', 1000):
#             print(LSK[[self.id, self.company, self.rate_of_return_type, self.support_value]])
#         LSK = LSK[[self.id, self.support_value]]
#         LSK[self.id] = LSK[self.id].apply(lambda x: f"({x})")
#         LSK = LSK.sort_values(by=self.id)
#         LSK = LSK.reset_index(drop=True)
#         #LSK = LSK[LSK[self.support_value] >= self.min_sup_val]
#
#         LS = LSK
#         LSK = LSK[[self.id]]
#
#
#         sequence_lenght = 1
#         while len(LSK) >1:
#             #CSK = pd.DataFrame(columns=['id'])
#
#             CSK_tree = tree.Tree(round(max_id)) #funkcja haszująca uzależniona jest od połowy ilości elementów
#             sequence_lenght = sequence_lenght + 1
#             #CSK_tree.support.drop(CSK_tree.support.index, inplace=True)
#
#             #Połączenie
#             if sequence_lenght == 2:
#                 print('Lenght 2 ----------------------------------------------------------------------------------')
#                 for x in LSK[self.id]:
#                     for y in LSK[self.id]:
#                         if True:
#                             #x,y
#                             new_row = pd.DataFrame({self.id: ['<' + str(x) + ',' + str(y) + '>']})
#                             #print(new_row)
#
#                             node = tree.Node()
#                             node.sequence_text = str(new_row[self.id][0])
#                             node.candidates = node.sequence_text
#                             CSK_tree.insert(node)
#
#                             #(y,x)
#                             new_row = pd.DataFrame({self.id: ['<' + str(y) + ',' + str(x) + '>']})
#                             #print(new_row)
#                             #CSK = pd.concat([CSK, new_row], ignore_index=True)
#                             node = tree.Node()
#                             node.sequence_text = str(new_row[self.id][0])
#                             node.candidates = node.sequence_text
#                             CSK_tree.insert(node)
#
#                             #(xy)
#                             if x != y:
#                                 new_row = pd.DataFrame({self.id: ['<' + str(x)[:-1] + ',' + str(y)[1:] + '>']})
#                                 #print(new_row)
#                                 #CSK = pd.concat([CSK, new_row], ignore_index=True)
#                                 node = tree.Node()
#                                 node.sequence_text = str(new_row[self.id][0])
#                                 node.candidates = node.sequence_text
#                                 CSK_tree.insert(node)
#                 #CSK = CSK.drop_duplicates(subset=['id'])
#                 can = CSK_tree.retrun()
#                 for c in can:
#                     for s in c.candidates:
#                         print(s)
#
#             elif sequence_lenght >= 3 and sequence_lenght <= 99:
#                 i = 1
#                 print('Lenght '+str(sequence_lenght) + '-----------------------------------------------------------------------')
#                 for S1 in LSK[self.id]:
#                     for S2 in LSK[self.id]:
#                         new_row = self.pars_sequences(S1, S2)
#                         lenseq = len(new_row[self.id])
#                         if lenseq > 0:
#                             #Eliminacja.
#                             # usuwamy i sprawdzamy pierwszy i ostatni znak
#                             # jeżeli jest sekwencja wieloelementowa to dla każdej weieloelementowej grupy usuwamy każdy znak.
#                             seq = self.generate_shorter_sequences(str(new_row[self.id][0]))
#                             not_in_lsk = False
#                             for iseq in seq:
#                                 if iseq not in LSK[self.id].values:
#                                     not_in_lsk = True
#                                     break
#                             #jeżeli nie wyeliminowany to można dodać do zbioru kandydatów
#                             if not_in_lsk == False:
#                                 #Dodanie kandydata do zbioru kandydatów
#                                 #print(seq)
#                                 #CSK = pd.concat([CSK, new_row], ignore_index=True)
#                                 node = tree.Node()
#                                 # wierzchołek
#                                 node.sequence_text = str(new_row[self.id][0])
#                                 node.candidates = node.sequence_text
#                                 CSK_tree.insert(node)
#
#             else:
#                 print('Error')
#                 break
#
#             if sequence_lenght >= 2:
#             # Przechodzenie przez dane i liczenie wystąpień kandydatów.
#                 CSK_tree.win_size = self.win_size
#                 CSK_tree.min_gap = self.min_gap
#                 CSK_tree.max_gap = self.max_gap
#                 CSK_tree.calculate(self.transaction_data_test)
#
#             if not CSK_tree.support.empty:
#                 CSK = CSK_tree.support.apply(tuple)
#                 CSK[self.id] = CSK[self.id].apply(lambda x: str(x))
#                 CSK = CSK[self.id].value_counts()
#                 #CSK = CSK.rename({'0': 'support_value'})
#                 CSK = CSK.reset_index()
#                 CSK = CSK.rename(columns={self.id: self.support_value})
#                 CSK = CSK.rename(columns={'index': self.id})
#                 with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
#                     print("Tabela CKS przed przycieciem" + str(sequence_lenght) + ":")
#                     print(CSK)
#                     print("Koniec petli")
#                 CSK = CSK[CSK[self.support_value] >= self.min_sup_val]
#                 with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
#                     print("Tabela CKS po przycieciu" + str(sequence_lenght) + ":")
#                     print(CSK)
#                     print("Koniec petli")
#
#             else:
#                 print("Empty table")
#                 break
#
#             #Złączenie listy sprawdzonych kandydatów z listą wszystkich sekwencji
#             LS = pd.concat([LS, CSK], ignore_index=True)
#             #Stworzenie nowej listy do następnej pętli
#             LSK = CSK[[self.id]]
#             #print("-------------------------------------------------------")
#
#
#             # #CSK = CSK.sort_values(by=['id'])
#             # with pd.option_context('display.max_rows', 5, 'display.max_columns', 5, 'display.width', 1000):
#             #     print("Tabela CKS "+str(sequence_lenght)+":")
#             #     print(CSK)
#             # #     print("Koniec pętli")
#
#
#
#         with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
#             #print(self.transaction_data_test)
#             #print(min_sup_table)
#             print(LS)
#             print('Koniec programu')
#         return LS
#
