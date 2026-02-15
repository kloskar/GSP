from dataclasses import dataclass
from typing import Dict, List, Tuple, FrozenSet, Optional, Set

Pattern = Tuple[FrozenSet[str], ...]
TimedSequenceInt = List[Tuple[int, Set[str]]]  # [(data_int, itemset), ...]


@dataclass
class TrieNode:
    children: Dict[FrozenSet[str], "TrieNode"]
    patterns: List[Pattern]

    def __init__(self) -> None:
        self.children = {}
        self.patterns = []


class PatternTrie:
    def __init__(self) -> None:
        self.root = TrieNode()
        self.first_index: Dict[FrozenSet[str], List[Pattern]] = {}

    def insert(self, pat: Pattern) -> None:
        if not pat:
            return
        self.first_index.setdefault(pat[0], []).append(pat)
        node = self.root
        for iset in pat:
            if iset not in node.children:
                node.children[iset] = TrieNode()
            node = node.children[iset]
        node.patterns.append(pat)

    def build(self, patterns: List[Pattern]) -> None:
        self.root = TrieNode()
        self.first_index = {}
        for p in patterns:
            self.insert(p)


def contains_with_int_constraints(
    seq: TimedSequenceInt,
    pat: Pattern,
    min_gap: int,
    max_gap: int,
    win_size: Optional[int] = None
) -> bool:
    """
    seq = [(t_int, itemset), ...] z rosnącym t_int (nie musi być ciągły).
    Warunki:
      - pat[i] ⊆ itemset w danych
      - min_gap <= (t_next - t_prev) <= max_gap
      - jeśli win_size: (t_last - t_first) <= win_size
    """
    if not pat:
        return True

    # pozycje w seq, gdzie pasuje każdy itemset wzorca
    positions: List[List[int]] = []
    for iset in pat:
        matches = [i for i, (_, X) in enumerate(seq) if iset.issubset(X)]
        if not matches:
            return False
        positions.append(matches)

    def dfs(k: int, prev_idx: int, first_t: int, prev_t: int) -> bool:
        if k == len(pat):
            return True
        for idx in positions[k]:
            if idx <= prev_idx:
                continue
            t = seq[idx][0]
            dt = t - prev_t
            if dt < min_gap or dt > max_gap:
                continue
            if win_size is not None and (t - first_t) > win_size:
                continue
            if dfs(k + 1, idx, first_t, t):
                return True
        return False

    for start_idx in positions[0]:
        t0 = seq[start_idx][0]
        if dfs(1, start_idx, t0, t0):
            return True

    return False


def count_support_db_int(
    DB: List[TimedSequenceInt],
    patterns: List[Pattern],
    min_gap: int,
    max_gap: int,
    win_size: Optional[int]
) -> Dict[Pattern, int]:
    """
    Support per sekwencja (okno): +1 jeśli wzorzec występuje w oknie.
    Optymalizacja: indeks po pierwszym itemsecie.
    """
    trie = PatternTrie()
    trie.build(patterns)

    sup = {p: 0 for p in patterns}

    for seq in DB:
        for first_iset, pats in trie.first_index.items():
            if not any(first_iset.issubset(X) for _, X in seq):
                continue
            for p in pats:
                if contains_with_int_constraints(seq, p, min_gap, max_gap, win_size):
                    sup[p] += 1
    return sup




#
# class Node:
#     def __init__(self):
#         #wierzchołek
#         self.key = None #liczba zakodowana
#         self.nodes = [] #lista wierzchołków
#         self.sequence_text = None
#         self.sequence_keys = []
#         #liść
#         self.candidates = [] #lista kandydatów, gdy liść
#         self.data = None #lista sekwencji, gdy liść
#
#     def find_node(self, key):
#         if self.nodes.__len__() > 0:
#             for row in self.nodes:
#                 if row.key == key:
#                     return row
#             return Node()
#         else:
#             #print("nodes in Node are empty")
#             return Node()
#
# class Tree:
#     def __init__(self, function_key):
#         self.root = Node()
#         self.function_key = function_key
#         self.max_gap = None
#         self.min_gap = None
#         self.win_size = None
#         self.support = pd.DataFrame(columns=['id'])
#
#         # definicja_kolumn
#         self.comapny = 'firma'
#         self.timestamp = 'data'
#         self.price = 'cena'
#         self.rate_of_return_value = 'stopa zwrotu'
#         self.rate_of_return_type = 'typ stopy zwrotu'
#         self.min_sup_val = 'minimalny wskaznik wsparcia'
#         self.id = 'sekwencja'
#         self.support_value = 'wskaznik wsparcia'
#
#     def hash(self, key):
#         return key % self.function_key
#
#     def insert(self, node):
#         #hashowanie sekwencji
#         numbers = re.findall(r'\d+', str(node.sequence_text))
#         for number in numbers:
#             ext = [self.hash(int(number))]
#             node.sequence_keys.extend(ext)
#         #przypisanie klucza do pierwszego elementu seqwencji
#         node.key = node.sequence_keys[0]
#         if self.root is None:
#             self.root = Node()
#         self._insert(self.root, node)
#
#     def _insert(self, parent, child):
#         #Jeżeli to nie ostatni element listy
#         founded_child = parent.find_node(child.key)
#         if len(child.sequence_keys) > 1:
#             # Jeżeli node nie istnieje lub lista jest pusta. Dodaj Node i przejdź niżej.
#             if founded_child.key is None:
#                 #Dodanie wierzchołka
#                 new_child = Node()
#                 new_child.key = child.key
#                 parent.nodes.append(new_child)
#                 #Przejście na kolejny element sekwencji
#                 child.sequence_keys = child.sequence_keys[1:]
#                 child.key = child.sequence_keys[0]
#                 self._insert(new_child, child)
#
#             #Jeżeli node istnieje. Przejdź do Node
#             elif founded_child.key is not None:
#                 child.sequence_keys = child.sequence_keys[1:]
#                 child.key = child.sequence_keys[0]
#                 self._insert(founded_child, child)
#
#         #Jeżeli to ostatni element listy
#         elif len(child.sequence_keys) == 1:
#             if founded_child.key is None:
#                 #Dodanie wierzchołka
#                 new_child = Node()
#                 new_child.key = child.key
#                 new_child.candidates.append(child.candidates)
#                 parent.nodes.append(new_child)
#
#             elif founded_child.key is not None:
#                 if child.candidates not in founded_child.candidates:
#                     founded_child.candidates.append(child.candidates)
#
#         else:
#             print("Wrong Node")
#
#     def retrun(self):
#         leaves = []
#         self._return(self.root, leaves)
#         return leaves
#
#     def _return(self, node, leaves):
#         if node is not None:
#             if len(node.candidates) > 0:
#                 leaves.append(node)
#             for node in node.nodes:
#                 self._return(node, leaves)
#
#     def find(self, seq):
#         seq_txt = seq
#         seq = re.findall(r'\d+', str(seq))
#         return self._find(self.root, seq, seq_txt)
#
#     def _find(self,node , seq, seq_txt):
#         if len(seq)>0:
#             key = self.hash(int(seq[0]))
#             seq = seq[1:]
#
#         if seq_txt in node.candidates:
#             return seq_txt
#         else:
#             found_node = node.find_node(key)
#             if found_node.key is None:
#                 return None
#             return self._find(found_node, seq, seq_txt)
#
#     def check_candidate(self, candidates, sequence_list):
#         ret = 0
#         # if candidates == '<(1,2,2)>':
#         #     print(1)
#         #print(candidates)
#         # Remove the angle brackets and split the string into tuples
#         candidates = candidates[1:-1]
#         sequence_list1 = sequence_list[[self.id]]
#         candidate_list = candidates.split("),(")
#         candidate_list = [tuple(map(int, seq.replace("(", "").replace(")", "").split(','))) for seq in candidate_list]
#         sequence_list = sequence_list.reset_index(drop=True)
#         canditate_list_index = 0
#         sequence_list_index = [0]
#         actual_list = []
#
#         first_candidate_counter = 0
#         #Dla każdego elementu sekwencji sprawdź czy zawiera się w danych.
#         while 0 <= canditate_list_index < candidate_list.__len__() :
#             if actual_list.__len__() == 0:
#                 is_first = True
#             else:
#                 is_first = False
#             sequence_list_index = self.find_seq_in_data(candidate_list[canditate_list_index], sequence_list[sequence_list_index[len(sequence_list_index)-1]:], is_first)
#
#             #Przechodzisz po kolejnych elementach kandydatów
#             if sequence_list_index[len(sequence_list_index)-1] >= 0:
#                 actual_list.extend(sequence_list_index)
#                 canditate_list_index = canditate_list_index + 1
#             #Powrót do poprzedniego elementu, przekroczony max_gap
#             elif sequence_list_index[0] == -1:
#                 break
#
#                 # # Jeżeli nie znalazał pierwszego elementu w pierwszym przejściu to brak sekwencji
#                 # if actual_list.__len__() == 0:
#                 #     first_candidate_counter = first_candidate_counter + 1
#                 #     if first_candidate_counter == len(sequence_list):
#                 #         break
#                 #     actual_list = []
#                 #     sequence_list_index = [first_candidate_counter]
#                 #
#                 # # # Poszukanie następnego wystąpienia od pierwszego elementu
#                 # # elif actual_list.__len__() == 1:
#                 # #     canditate_list_index = 0
#                 # #     sequence_list_index = [actual_list[0]]
#                 #
#                 # #"Cofniecie się do poprzedniego wyrazu i próba znalezienia następnego wyrazu w max_gap, jeżeli przekroczono max_gap"
#                 # elif actual_list.__len__() >= 1:
#                 #     sequence_list_index = [actual_list[actual_list.__len__()-1]+1]
#                 #     #sequence_list_index = [sequence_list_index + 1]
#                 #     if sequence_list_index[0] > sequence_list.index.max():
#                 #         break #Jeżeli obiekt jest poza horyzontem to brak zawierania się w sekwencji.
#                 #     actual_list = actual_list[:-1]
#                 #     canditate_list_index = canditate_list_index - 1
#
#         searched_list = []
#         for lst in actual_list:
#             searched_list.append(sequence_list.iloc[lst][self.id])
#
#         candidate_list_numbers = []
#         for x in candidate_list:
#             for y in x:
#                 if isinstance(y, (int, float)):  # Sprawdzenie, czy element jest liczbą
#                     candidate_list_numbers.append(y)
#
#         if searched_list == candidate_list_numbers:
#             ret = 1
#             #row1 = pd.DataFrame([{'id': [candidates]}])
#             #self.support = pd.concat([self.support, row1])
#             #print("Zawiera sie")
#         elif sequence_list_index == -99:
#             ret = 2
#         else:
#             ret = 0
#             #print("Nie zawiera sie")
#         return ret
#
#     def find_multiseq_in_data(self, candidate, sequence_list, found_sequence=[]):
#         ret = [-1]
#         can_counter = 0
#         seq_counter = 0
#         if candidate.__len__() != list(dict.fromkeys(candidate)).__len__():
#             ret = [-1]
#         elif candidate.__len__()>1:
#             for can in candidate:
#                 can_counter = can_counter + 1
#                 for index in sequence_list.index:
#                     seq_counter = seq_counter + 1
#                     if found_sequence.__len__() == 0 and sequence_list.loc[index][self.id] == can:
#                         found_sequence.append(index)
#                         return self.find_multiseq_in_data(candidate[can_counter:], sequence_list[seq_counter-1:], found_sequence)
#                     elif sequence_list.loc[index][self.id] != can:
#                         continue
#                     else:
#                         last_can_date_min = sequence_list.loc[found_sequence[found_sequence.__len__() - 1]][self.timestamp] - pd.Timedelta(self.win_size, unit='h')
#                         last_can_date_max = sequence_list.loc[found_sequence[found_sequence.__len__() - 1]][self.timestamp] + pd.Timedelta(self.win_size, unit='h')
#                         seq_date = sequence_list.loc[index][self.timestamp]
#                         if sequence_list.loc[index][self.id] == can and last_can_date_min <=seq_date<= last_can_date_max:
#                             found_sequence.append(index)
#                             candidate_x = candidate[can_counter:]
#                             sequence_list_x = sequence_list[seq_counter - 1:]
#                             return self.find_multiseq_in_data(candidate[can_counter:], sequence_list[seq_counter-1:], found_sequence)
#                 break
#         elif candidate.__len__()==1:
#             last_can_date_min = sequence_list.loc[found_sequence[found_sequence.__len__() - 1]][self.timestamp] - pd.Timedelta(self.win_size, unit='h')
#             last_can_date_max = sequence_list.loc[found_sequence[found_sequence.__len__() - 1]][self.timestamp] + pd.Timedelta(self.win_size, unit='h')
#             for index in sequence_list.index:
#                 seq_date = sequence_list.loc[index][self.timestamp]
#                 if sequence_list.loc[index][self.id] == candidate[0] and last_can_date_min <= seq_date <= last_can_date_max:
#                     found_sequence.append(index)
#                     break
#         if found_sequence.__len__()>0:
#             ret = found_sequence
#         return ret
#
#
#
#     def find_seq_in_data(self, candidate, sequence_list, is_first=True):
#         ret = [-1]
#         candidate_date = sequence_list.iloc[0][self.timestamp]
#         max_gap_date = candidate_date + pd.Timedelta(self.max_gap, unit='h')
#         min_gap_date = candidate_date + pd.Timedelta(self.min_gap, unit='h')
#         if candidate.__len__() == 1:
#             #print("Len 1")
#             for sequence in sequence_list.index:
#                 sequence_date = sequence_list.loc[sequence][self.timestamp]
#                 if candidate[0] == sequence_list.loc[sequence][self.id] and sequence_date <= max_gap_date :#'''candidate_date <='''
#                     if min_gap_date <= sequence_date or is_first == True:
#                         ret = [sequence]
#                         break
#                 #Przekroczony max gap
#                 elif sequence_date > max_gap_date:
#                     ret = [-1]
#                     break
#
#         elif candidate.__len__() >  1:
#             #print("Len >1")
#             ret = self.find_multiseq_in_data(candidate, sequence_list, found_sequence=[])
#
#         return ret
#
#
#
#
#     def calculate(self, data):
#         for index, row in data.iterrows():
#             x_hash = self.hash(row[self.id])
#             node = self.root.find_node(x_hash)
#             print('')
#             print('-------------------------------------------------------------------------')
#             print("Start:" + str(row[self.id]))
#             data_sequence = [row['index']]
#             self.calculate_node(node, row, data, data_sequence, lvl = [0])
#         #print("Koniec")
#
#     def calculate_node(self, parent, row, data, data_sequence, lvl):
#         #jeżeli są kolejne wierzchołki do sprawdzenia
#         if len(parent.nodes) > 0:
#             data_len = len(data)
#             row_start_index = row['index']
#             row_next_index = row['index']
#             #Cofamy się do początku okna
#             # while row_next_index >=0:
#             #     row_next_index = row_next_index - 1
#             #     if row['timestamp'] - pd.Timedelta(self.win_size, unit='h') < data.iloc[row_next_index]['timestamp']:
#             #         row_next_index = row_next_index + 1
#             #         break
#             #sprawdzamy czy index następnego wiersza nie przekracza wielkości tabeli
#             if data_len >= row_next_index:
#                 #dla kazdego wiersza w danych, który jest w zakresie maksymalnego odstępu czasowego rekurencyjnie sprawdzamy czy istnieje.
#                 for x in range(row_next_index, data_len):
#                     if row_start_index == x:
#                         continue
#                     #jeżeli czas przkracza maksymalny odstęp to zakańczamy pętle, ponieważ posortowane dane gwarantują brak następnych danych, kóre się zawierają.
#                     row_max_timestamp = row[self.timestamp] + pd.Timedelta(max(self.max_gap, self.win_size), unit='h')
#                     if row_max_timestamp < data.iloc[x][self.timestamp]:
#                         break
#                     next_row = data.loc[x]
#                     x_hash = self.hash(next_row[self.id])
#                     child = parent.find_node(x_hash)
#                     if child.key is None: # przerywa sprawdzanie dla x, ponieważ nie znalazło wierzchołka
#                         continue
#                     data_sequence.append(next_row['index'])
#                     print("Wierzcholek: " + str(parent.key) )
#                     #print("Przejście do wierzchołka: " + str(child.key))
#                     new_lvl = lvl
#                     lvls = ', '.join(map(str, new_lvl))
#                     #print(f"Lvl = {lvls}")
#                     self.calculate_node(child, next_row, data, data_sequence, new_lvl)
#                     data_sequence.pop()
#
#
#         #jeżeli nie było następników a wierzchołek zawiera kandydatów, to dla każdego kandydata sprawdzamy, czy się zawiera.
#         elif len(parent.candidates) > 0:
#             #print("Liczenie zawierania sie sekwencji w danych:")
#             #sprawdzamy czy każdy kandydat zawiera się w sekwencji
#             seq_list = data.loc[data_sequence, ['index', self.id, self.timestamp]]
#             # print('')
#             # print('-------------------------------------------------------------------------')
#             print(f"Lisc: " + str(parent.key) + " Kandydaci: " + str(parent.candidates))
#             for candidate in parent.candidates:
#                 values = ', '.join(map(str, seq_list[self.id]))
#                 #print(f"Sprawdzanie: "+str(candidate)+f" w <{values}>")
#                 is_con = self.check_candidate(candidate, seq_list)
#                 if is_con == 1:
#                     #print(f"Czy zawiera: (tak) " +str(candidate))
#                     row = pd.DataFrame({self.id: [candidate]})
#                     self.support = pd.concat([self.support, row], ignore_index=True)
#                 else:
#                     a = 1
#                     #print(f"Nie zawiera: (nie) " +str(candidate))
#
#
#
#         #else:
#             #print("Brak kandydatow i wierzcholkow"+ str(parent.key))
#
#
#
#
# # tree = Tree(3)
# #
# # #First Node
# # seq1 = ['<(11),(2),(3)>']
# # node = Node()
# # # wierzchołek
# # node.sequence_text = seq1
# # node.candidates = node.sequence_text
# # tree.insert(node)
# # seq2 = ['<(11),(2),(6)>']
# # node2 = Node()
# # # wierzchołek
# # node2.sequence_text = seq2
# # node2.candidates = node2.sequence_text
# # tree.insert(node2)
# # seq3 = ['<(11),(2),(9)>']
# # node3 = Node()
# # # wierzchołek
# # node3.sequence_text = seq3
# # node3.candidates = node3.sequence_text
# # tree.insert(node3)
# #
# # leaves = tree.retrun()
# #
# # find = tree.find(seq2)
# # find2 = tree.find(['<(11),(2),(7)>'])
#
#
#
# #print('koniec')