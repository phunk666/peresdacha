from typing import Set, List, Tuple, Callable
from copy import deepcopy

class Relation:
    def __init__(self, name: str, attributes: List[str], data: Set[Tuple]):
        self.name = name
        self.attributes = attributes
        self.data = data
        
        for tuple_data in data:
            if len(tuple_data) != len(attributes):
                raise ValueError(f"Кортеж {tuple_data} не соответствует атрибутам {attributes}")
    
    def __str__(self):
        header = f"Отношение: {self.name} | Атрибуты: {self.attributes}\n"
        data_str = "\n".join([str(row) for row in self.data])
        return header + data_str

class Relations:
    @staticmethod
    def rename(relation: Relation, old_name: str, new_name: str) -> Relation:
        if old_name not in relation.attributes:
            raise ValueError(f"Атрибут '{old_name}' не найден")
        
        new_attrs = [new_name if attr == old_name else attr for attr in relation.attributes]
        return Relation(f"{relation.name}_renamed", new_attrs, deepcopy(relation.data))
    
    @staticmethod
    def union(rel_a: Relation, rel_b: Relation) -> Relation:
        if rel_a.attributes != rel_b.attributes:
            raise ValueError("Атрибуты отношений не совпадают для объединения")
        
        new_data = rel_a.data.union(rel_b.data)
        return Relation(f"{rel_a.name}_UNION_{rel_b.name}", rel_a.attributes, new_data)
    
    @staticmethod
    def intersection(rel_a: Relation, rel_b: Relation) -> Relation:
        if rel_a.attributes != rel_b.attributes:
            raise ValueError("Атрибуты отношений не совпадают")
        
        new_data = rel_a.data.intersection(rel_b.data)
        return Relation(f"{rel_a.name}_INTERSECT_{rel_b.name}", rel_a.attributes, new_data)
    
    @staticmethod
    def difference(rel_a: Relation, rel_b: Relation) -> Relation:
        if rel_a.attributes != rel_b.attributes:
            raise ValueError("Атрибуты отношений не совпадают")
        
        new_data = rel_a.data.difference(rel_b.data)
        return Relation(f"{rel_a.name}_MINUS_{rel_b.name}", rel_a.attributes, new_data)
    
    @staticmethod
    def assign(relation: Relation, new_name: str) -> Relation:
        return Relation(new_name, relation.attributes.copy(), deepcopy(relation.data))
    
    @staticmethod
    def cartesian_product(rel_a: Relation, rel_b: Relation) -> Relation:
        new_attrs = rel_a.attributes + rel_b.attributes
        new_data = set()
        
        for tuple_a in rel_a.data:
            for tuple_b in rel_b.data:
                new_data.add(tuple_a + tuple_b)
        
        return Relation(f"{rel_a.name}_x_{rel_b.name}", new_attrs, new_data)
    
    @staticmethod
    def selection(relation: Relation, condition: Callable[[Tuple], bool]) -> Relation:
        selected_data = {row for row in relation.data if condition(row)}
        return Relation(f"{relation.name}_SELECT", relation.attributes, selected_data)
    
    @staticmethod
    def projection(relation: Relation, keep_attrs: List[str]) -> Relation:
        indices = [relation.attributes.index(attr) for attr in keep_attrs]
        
        new_data = set()
        for row in relation.data:
            projected_row = tuple(row[i] for i in indices)
            new_data.add(projected_row)
        
        return Relation(f"{relation.name}_PROJ", keep_attrs, new_data)
    
    @staticmethod
    def natural_join(rel_a: Relation, rel_b: Relation) -> Relation:
        common_attrs = [attr for attr in rel_a.attributes if attr in rel_b.attributes]
        
        if not common_attrs:
            return Relations.cartesian_product(rel_a, rel_b)
        
        a_common_indices = [rel_a.attributes.index(attr) for attr in common_attrs]
        b_common_indices = [rel_b.attributes.index(attr) for attr in common_attrs]
        
        new_attrs = rel_a.attributes + [attr for attr in rel_b.attributes if attr not in common_attrs]
        
        b_index = {}
        for i, row_b in enumerate(rel_b.data):
            key = tuple(row_b[idx] for idx in b_common_indices)
            if key not in b_index:
                b_index[key] = []
            b_index[key].append(row_b)
        
        new_data = set()
        for row_a in rel_a.data:
            key = tuple(row_a[idx] for idx in a_common_indices)
            if key in b_index:
                for row_b in b_index[key]:
                    row_b_unique = tuple(row_b[j] for j in range(len(rel_b.attributes)) if j not in b_common_indices)
                    new_data.add(row_a + row_b_unique)
        
        return Relation(f"{rel_a.name}_JOIN_{rel_b.name}", new_attrs, new_data)
    
    @staticmethod
    def division(rel_a: Relation, rel_b: Relation) -> Relation:
        common_attrs = [attr for attr in rel_b.attributes if attr in rel_a.attributes]
        
        if set(rel_b.attributes) != set(common_attrs):
            raise ValueError("Атрибуты B должны быть подмножеством атрибутов A")
        
        result_attrs = [attr for attr in rel_a.attributes if attr not in common_attrs]
        if not result_attrs:
            raise ValueError("После деления не остается атрибутов")
        
        a_common_indices = [rel_a.attributes.index(attr) for attr in common_attrs]
        a_result_indices = [rel_a.attributes.index(attr) for attr in result_attrs]
        
        all_x_values = {tuple(row[idx] for idx in a_result_indices) for row in rel_a.data}
        
        b_tuples = rel_b.data
        
        a_grouped = {}
        for row in rel_a.data:
            x = tuple(row[idx] for idx in a_result_indices)
            y = tuple(row[idx] for idx in a_common_indices)
            if x not in a_grouped:
                a_grouped[x] = set()
            a_grouped[x].add(y)
        
        result_data = {x for x, y_set in a_grouped.items() if y_set.issuperset(b_tuples)}
        
        return Relation(f"{rel_a.name}_DIV_{rel_b.name}", result_attrs, result_data)

if __name__ == "__main__":
    students = Relation("Students", 
                        ["id", "name", "group_id"], 
                        {(1, "Иван", 101), (2, "Мария", 102), (3, "Петр", 101)})
    
    groups = Relation("Groups",
                      ["group_id", "course", "faculty"],
                      {(101, 2, "ИТ"), (102, 3, "ИТ")})
    
    print("=== Демонстрация реляционных операций ===")
    print(students)
    print()
    print(groups)
    print()
    
    proj = Relations.projection(students, ["name"])
    print("1. Проекция (имена):\n", proj)
    
    sel = Relations.selection(students, lambda row: row[2] == 101)
    print("\n2. Выборка (группа 101):\n", sel)
    
    join = Relations.natural_join(students, groups)
    print("\n3. Естественное соединение Students ⨝ Groups:\n", join)
    
    renamed = Relations.rename(students, "name", "student_name")
    print("\n4. Переименование атрибута:\n", renamed)
