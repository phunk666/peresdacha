# relations.py
from typing import Set, Dict, List, Any, Tuple, Callable
from copy import deepcopy

class Relation:
    """Простая реализация отношения (таблицы)"""
    def __init__(self, name: str, attributes: List[str], data: Set[Tuple]):
        """
        :param name: Имя отношения
        :param attributes: Список имен атрибутов (столбцов)
        :param data: Множество кортежей (данные)
        """
        self.name = name
        self.attributes = attributes
        self.data = data
        
        # Валидация: каждый кортеж должен соответствовать длине attributes
        for tuple_data in data:
            if len(tuple_data) != len(attributes):
                raise ValueError(f"Кортеж {tuple_data} не соответствует атрибутам {attributes}")
    
    def __str__(self):
        header = f"Отношение: {self.name} | Атрибуты: {self.attributes}\n"
        data_str = "\n".join([str(row) for row in self.data])
        return header + data_str

class Relations:
    """
    Класс, реализующий операции реляционной алгебры.
    """
    
    @staticmethod
    def rename(relation: Relation, old_name: str, new_name: str) -> Relation:
        """
        Переименование атрибута.
        """
        if old_name not in relation.attributes:
            raise ValueError(f"Атрибут '{old_name}' не найден")
        
        new_attrs = [new_name if attr == old_name else attr for attr in relation.attributes]
        return Relation(f"{relation.name}_renamed", new_attrs, deepcopy(relation.data))
    
    @staticmethod
    def union(rel_a: Relation, rel_b: Relation) -> Relation:
        """
        Объединение (совместимые по атрибутам отношения).
        """
        if rel_a.attributes != rel_b.attributes:
            raise ValueError("Атрибуты отношений не совпадают для объединения")
        
        new_data = rel_a.data.union(rel_b.data)
        return Relation(f"{rel_a.name}_UNION_{rel_b.name}", rel_a.attributes, new_data)
    
    @staticmethod
    def intersection(rel_a: Relation, rel_b: Relation) -> Relation:
        """
        Пересечение.
        """
        if rel_a.attributes != rel_b.attributes:
            raise ValueError("Атрибуты отношений не совпадают")
        
        new_data = rel_a.data.intersection(rel_b.data)
        return Relation(f"{rel_a.name}_INTERSECT_{rel_b.name}", rel_a.attributes, new_data)
    
    @staticmethod
    def difference(rel_a: Relation, rel_b: Relation) -> Relation:
        """
        Разность (Вычитание).
        """
        if rel_a.attributes != rel_b.attributes:
            raise ValueError("Атрибуты отношений не совпадают")
        
        new_data = rel_a.data.difference(rel_b.data)
        return Relation(f"{rel_a.name}_MINUS_{rel_b.name}", rel_a.attributes, new_data)
    
    @staticmethod
    def assign(relation: Relation, new_name: str) -> Relation:
        """
        Операция присваивания (создание копии с новым именем).
        """
        return Relation(new_name, relation.attributes.copy(), deepcopy(relation.data))
    
    @staticmethod
    def cartesian_product(rel_a: Relation, rel_b: Relation) -> Relation:
        """
        Декартово произведение.
        Атрибуты объединяются, кортежи комбинируются.
        """
        new_attrs = rel_a.attributes + rel_b.attributes
        new_data = set()
        
        for tuple_a in rel_a.data:
            for tuple_b in rel_b.data:
                new_data.add(tuple_a + tuple_b)
        
        return Relation(f"{rel_a.name}_x_{rel_b.name}", new_attrs, new_data)
    
    @staticmethod
    def selection(relation: Relation, condition: Callable[[Tuple], bool]) -> Relation:
        """
        Выборка (Ограничение, WHERE).
        """
        selected_data = {row for row in relation.data if condition(row)}
        return Relation(f"{relation.name}_SELECT", relation.attributes, selected_data)
    
    @staticmethod
    def projection(relation: Relation, keep_attrs: List[str]) -> Relation:
        """
        Проекция (SELECT DISTINCT в SQL).
        """
        # Определяем индексы атрибутов, которые нужно оставить
        indices = [relation.attributes.index(attr) for attr in keep_attrs]
        
        new_data = set()
        for row in relation.data:
            projected_row = tuple(row[i] for i in indices)
            new_data.add(projected_row)
        
        return Relation(f"{relation.name}_PROJ", keep_attrs, new_data)
    
    @staticmethod
    def natural_join(rel_a: Relation, rel_b: Relation) -> Relation:
        """
        Естественное соединение (по общим атрибутам).
        """
        common_attrs = [attr for attr in rel_a.attributes if attr in rel_b.attributes]
        
        if not common_attrs:
            # Если общих атрибутов нет, это декартово произведение
            return Relations.cartesian_product(rel_a, rel_b)
        
        # Индексы общих атрибутов в обоих отношениях
        a_common_indices = [rel_a.attributes.index(attr) for attr in common_attrs]
        b_common_indices = [rel_b.attributes.index(attr) for attr in common_attrs]
        
        # Атрибуты результата: все уникальные атрибуты из A и B
        new_attrs = rel_a.attributes + [attr for attr in rel_b.attributes if attr not in common_attrs]
        
        # Строим индекс для быстрого поиска по общим атрибутам в rel_b
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
                    # Убираем дублирующиеся общие атрибуты из row_b
                    row_b_unique = tuple(row_b[j] for j in range(len(rel_b.attributes)) if j not in b_common_indices)
                    new_data.add(row_a + row_b_unique)
        
        return Relation(f"{rel_a.name}_JOIN_{rel_b.name}", new_attrs, new_data)
    
    @staticmethod
    def division(rel_a: Relation, rel_b: Relation) -> Relation:
        """
        Операция деления (A ÷ B).
        Возвращает все значения x такие, что для каждого y в B, пара (x, y) есть в A.
        Предполагается, что все атрибуты B являются подмножеством атрибутов A.
        """
        # Атрибуты A делятся на общие (X) и те, что только в A (Y)
        common_attrs = [attr for attr in rel_b.attributes if attr in rel_a.attributes]
        
        if set(rel_b.attributes) != set(common_attrs):
            raise ValueError("Атрибуты B должны быть подмножеством атрибутов A")
        
        # Атрибуты, которые останутся в результате (A - B)
        result_attrs = [attr for attr in rel_a.attributes if attr not in common_attrs]
        if not result_attrs:
            raise ValueError("После деления не остается атрибутов")
        
        # Индексы атрибутов
        a_common_indices = [rel_a.attributes.index(attr) for attr in common_attrs]
        a_result_indices = [rel_a.attributes.index(attr) for attr in result_attrs]
        
        # Все возможные значения result_attrs
        all_x_values = {tuple(row[idx] for idx in a_result_indices) for row in rel_a.data}
        
        # Множество всех комбинаций (x, y) из B (т.е. просто кортежи B)
        b_tuples = rel_b.data
        
        # Группируем A по X
        a_grouped = {}
        for row in rel_a.data:
            x = tuple(row[idx] for idx in a_result_indices)
            y = tuple(row[idx] for idx in a_common_indices)
            if x not in a_grouped:
                a_grouped[x] = set()
            a_grouped[x].add(y)
        
        # Результат: те X, для которых все y из B присутствуют в группе
        result_data = {x for x, y_set in a_grouped.items() if y_set.issuperset(b_tuples)}
        
        return Relation(f"{rel_a.name}_DIV_{rel_b.name}", result_attrs, result_data)

# Демонстрация работы
if __name__ == "__main__":
    # Отношение Студенты
    students = Relation("Students", 
                        ["id", "name", "group_id"], 
                        {(1, "Иван", 101), (2, "Мария", 102), (3, "Петр", 101)})
    
    # Отношение Группы
    groups = Relation("Groups",
                      ["group_id", "course", "faculty"],
                      {(101, 2, "ИТ"), (102, 3, "ИТ")})
    
    print("=== Демонстрация реляционных операций ===")
    print(students)
    print()
    print(groups)
    print()
    
    # 1. Проекция (имена студентов)
    proj = Relations.projection(students, ["name"])
    print("1. Проекция (имена):\n", proj)
    
    # 2. Выборка (студенты группы 101)
    sel = Relations.selection(students, lambda row: row[2] == 101)
    print("\n2. Выборка (группа 101):\n", sel)
    
    # 3. Естественное соединение
    join = Relations.natural_join(students, groups)
    print("\n3. Естественное соединение Students ⨝ Groups:\n", join)
    
    # 4. Переименование
    renamed = Relations.rename(students, "name", "student_name")
    print("\n4. Переименование атрибута:\n", renamed)
