from typing import Set, TypeVar, Tuple

T = TypeVar('T')
U = TypeVar('U')

class SetOperations:
    @staticmethod
    def union(set_a: Set[T], set_b: Set[U]) -> Set[T | U]:
        return set_a.union(set_b)
    
    @staticmethod
    def intersection(set_a: Set[T], set_b: Set[U]) -> Set[T | U]:
        return set_a.intersection(set_b)
    
    @staticmethod
    def difference(set_a: Set[T], set_b: Set[U]) -> Set[T]:
        return set_a.difference(set_b)
    
    @staticmethod
    def symmetric_difference(set_a: Set[T], set_b: Set[U]) -> Set[T | U]:
        return set_a.symmetric_difference(set_b)
    
    @staticmethod
    def complement(universal_set: Set[T], subset: Set[T]) -> Set[T]:
        return universal_set.difference(subset)
    
    @staticmethod
    def cartesian_product(set_a: Set[T], set_b: Set[U]) -> Set[Tuple[T, U]]:
        result = set()
        for a in set_a:
            for b in set_b:
                result.add((a, b))
        return result

if __name__ == "__main__":
    A = {1, 2, 3, 4}
    B = {3, 4, 5, 6}
    U = {1, 2, 3, 4, 5, 6, 7, 8}
    
    print("Множество A:", A)
    print("Множество B:", B)
    print("-" * 30)
    print("1. Объединение:", SetOperations.union(A, B))
    print("2. Пересечение:", SetOperations.intersection(A, B))
    print("3. Разность (A \\ B):", SetOperations.difference(A, B))
    print("4. Симметрическая разность:", SetOperations.symmetric_difference(A, B))
    print("5. Дополнение A (отн. U):", SetOperations.complement(U, A))
    print("6. Декартово произведение A x B:", SetOperations.cartesian_product(A, B))
