import heapq
import os
from typing import List

class ExternalMergeSort:
    def __init__(self, chunk_size: int = 1000):
        self.chunk_size = chunk_size
    
    def sort_file(self, input_file: str, output_file: str):
        """Основной метод сортировки"""
        print(f"🔄 Начинаем сортировку {input_file} -> {output_file}")
        
        # Фаза 1: Создание отсортированных чанков
        temp_files = self._create_sorted_chunks(input_file)
        print(f"✅ Создано {len(temp_files)} чанков")
        
        # Фаза 2: Слияние чанков
        self._merge_chunks(temp_files, output_file)
        print(f"✅ Сортировка завершена: {output_file}")
        
        # Очистка временных файлов
        self._cleanup(temp_files)
        print("🧹 Временные файлы удалены")
    
    def _create_sorted_chunks(self, input_file: str) -> List[str]:
        """Создание отсортированных чанков"""
        temp_files = []
        chunk = []
        file_counter = 0
        
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    chunk.append(line.strip())
                    if len(chunk) >= self.chunk_size:
                        chunk.sort()
                        temp_file = f"temp_{file_counter}.txt"
                        with open(temp_file, 'w', encoding='utf-8') as tf:
                            for item in chunk:
                                tf.write(item + '\n')
                        temp_files.append(temp_file)
                        print(f"📝 Чанк {file_counter + 1}: {len(chunk)} строк")
                        chunk.clear()
                        file_counter += 1
            
            # Последний чанк
            if chunk:
                chunk.sort()
                temp_file = f"temp_{file_counter}.txt"
                with open(temp_file, 'w', encoding='utf-8') as tf:
                    for item in chunk:
                        tf.write(item + '\n')
                temp_files.append(temp_file)
                print(f"📝 Последний чанк {file_counter + 1}: {len(chunk)} строк")
                
        except FileNotFoundError:
            print(f"❌ Файл {input_file} не найден!")
            return []
        
        return temp_files
    
    def _merge_chunks(self, temp_files: List[str], output_file: str):
        """K-стороннее слияние с использованием min-heap"""
        heap = []
        
        # Открываем все файлы заранее
        file_handles = []
        for i, temp_file in enumerate(temp_files):
            try:
                f = open(temp_file, 'r', encoding='utf-8')
                first_line = f.readline().strip()
                if first_line:
                    heapq.heappush(heap, (first_line, i, 0, f))
                    file_handles.append(f)
                else:
                    f.close()
                    file_handles.append(None)
            except Exception as e:
                print(f"❌ Ошибка чтения {temp_file}: {e}")
        
        with open(output_file, 'w', encoding='utf-8') as out:
            lines_written = 0
            while heap:
                value, file_idx, line_idx, file_handle = heapq.heappop(heap)
                out.write(value + '\n')
                lines_written += 1
                
                # Читаем следующую строку
                next_line = file_handle.readline().strip()
                if next_line:
                    heapq.heappush(heap, (next_line, file_idx, line_idx + 1, file_handle))
            
            print(f"📊 Записано строк в итоговый файл: {lines_written}")
    
    def _cleanup(self, temp_files: List[str]):
        """Удаление временных файлов"""
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

def create_test_file(filename: str = "test_input.txt", lines: int = 10000):
    """Создает тестовый файл с случайными числами"""
    import random
    with open(filename, 'w', encoding='utf-8') as f:
        for _ in range(lines):
            f.write(f"{random.randint(1, 1000000)}\n")
    print(f"✅ Создан тестовый файл {filename} ({lines} строк)")

# Интерактивное меню
def main():
    print("🎯 External Merge Sort - Интерактивная версия")
    print("=" * 50)
    
    while True:
        print("\n📋 Выберите действие:")
        print("1. Создать тестовый файл")
        print("2. Отсортировать файл")
        print("3. Показать файлы в папке")
        print("4. Выход")
        
        choice = input("\nВаш выбор (1-4): ").strip()
        
        if choice == '1':
            size = input("Размер файла (строк, по умолчанию 10000): ").strip()
            size = int(size) if size.isdigit() else 10000
            create_test_file("test_input.txt", size)
        
        elif choice == '2':
            input_file = input("Входной файл (по умолчанию test_input.txt): ").strip() or "test_input.txt"
            output_file = input("Выходной файл (по умолчанию sorted_output.txt): ").strip() or "sorted_output.txt"
            
            sorter = ExternalMergeSort(chunk_size=1000)
            sorter.sort_file(input_file, output_file)
        
        elif choice == '3':
            print("\n📁 Файлы в текущей папке:")
            for file in os.listdir('.'):
                if file.endswith('.txt'):
                    size = os.path.getsize(file)
                    print(f"  {file} ({size} байт)")
        
        elif choice == '4':
            print("👋 До свидания!")
            break
        
        else:
            print("❌ Неверный выбор!")

if __name__ == "__main__":
    main()
