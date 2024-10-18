import os
import pandas as pd
import pyreadstat
import pyreadr

# Функция для обработки файлов и сохранения в .xlsx
def process_files(folder_path):
    # Ищем файлы с нужными расширениями
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_name, file_extension = os.path.splitext(file)

            # Обработка .dta файлов
            if file_extension == ".dta":
                df, meta = pyreadstat.read_dta(file_path)
                output_path = os.path.join(root, f"{file_name}.xlsx")
                df.to_excel(output_path, index=False)
                print(f"Сохранен файл: {output_path}")

            # Обработка .rdata файлов
            elif file_extension == ".rdata":
                result = pyreadr.read_r(file_path)
                df = list(result.values())[0]  # получаем датафрейм
                output_path = os.path.join(root, f"{file_name}.xlsx")
                df.to_excel(output_path, index=False)
                print(f"Сохранен файл: {output_path}")

            # Обработка .sav файлов
            elif file_extension == ".sav":
                df, meta = pyreadstat.read_sav(file_path)
                output_path = os.path.join(root, f"{file_name}.xlsx")
                df.to_excel(output_path, index=False)
                print(f"Сохранен файл: {output_path}")

root_folder = r'C:\Users\BG\Desktop\СРСП week 6. Иванников Артём. БИ2209'
process_files(root_folder)
