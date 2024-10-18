import os
import pandas as pd
import dask.dataframe as dd

# Функция для обработки каждого Excel файла
def process_excel_file(file_path):
    # Загружаем данные с помощью pandas
    df = pd.read_excel(file_path)

    # Преобразуем в Dask DataFrame
    ddf = dd.from_pandas(df, npartitions=1)

    # Проверка наличия необходимых столбцов
    required_columns = ['wt', 'N_adults', 'N_child', 'Prob_Mod_Sev', 'Prob_sev']
    for col in required_columns:
        if col not in ddf.columns:
            raise ValueError(f"Столбец {col} отсутствует в данных файла {file_path}")

    # Преобразование данных в числовые значения
    for col in ['wt', 'N_adults', 'N_child']:
        ddf[col] = dd.to_numeric(ddf[col], errors='coerce')  # Преобразуем с заменой ошибок на NaN

    # Удаляем строки с NaN значениями в критически важных столбцах
    ddf = ddf.dropna(subset=['wt', 'N_adults', 'N_child', 'Prob_Mod_Sev', 'Prob_sev'])

    # 1. Рассчитываем переменную children weight
    ddf['children_weight'] = ddf['wt'] / ddf['N_adults'] * ddf['N_child']

    # 2. Рассчитываем F_mod+sev_ad и F_sev_ad (для взрослых)
    F_mod_sev_ad = (ddf['Prob_Mod_Sev'] * ddf['wt']).sum() / ddf['wt'].sum()
    F_sev_ad = (ddf['Prob_sev'] * ddf['wt']).sum() / ddf['wt'].sum()

    # 3. Рассчитываем F_mod+sev_child и F_sev_child (для детей)
    F_mod_sev_child = (ddf['Prob_Mod_Sev'] * ddf['children_weight']).sum() / ddf['children_weight'].sum()
    F_sev_child = (ddf['Prob_sev'] * ddf['children_weight']).sum() / ddf['children_weight'].sum()

    # 4. Рассчитываем общие значения для всех возрастов
    Pop_ad = ddf['wt'].sum()
    Pop_child = ddf['children_weight'].sum()

    Pop_mod_sev_ad = F_mod_sev_ad.compute() * Pop_ad.compute()
    Pop_mod_sev_child = F_mod_sev_child.compute() * Pop_child.compute()
    F_mod_sev_tot = (Pop_mod_sev_ad + Pop_mod_sev_child) / (Pop_ad.compute() + Pop_child.compute())

    Pop_sev_ad = F_sev_ad.compute() * Pop_ad.compute()
    Pop_sev_child = F_sev_child.compute() * Pop_child.compute()
    F_sev_tot = (Pop_sev_ad + Pop_sev_child) / (Pop_ad.compute() + Pop_child.compute())

    # Возвращаем результаты для текущего файла
    return {
        'F_mod_sev_ad': F_mod_sev_ad.compute(),
        'F_sev_ad': F_sev_ad.compute(),
        'F_mod_sev_child': F_mod_sev_child.compute(),
        'F_sev_child': F_sev_child.compute(),
        'F_mod_sev_tot': F_mod_sev_tot,
        'F_sev_tot': F_sev_tot
    }

# Основная функция для обхода всех папок и обработки всех файлов
def process_all_files(root_folder):
    results = []

    # Проходим по странам и годам
    for country in os.listdir(root_folder):
        country_path = os.path.join(root_folder, country)
        
        # Проходим по годам в каждой стране
        if os.path.isdir(country_path):
            for year in os.listdir(country_path):
                year_path = os.path.join(country_path, year)
                
                if os.path.isdir(year_path):
                    # Ищем Excel файл в папке года
                    for file in os.listdir(year_path):
                        if file.endswith('.xlsx'):
                            file_path = os.path.join(year_path, file)
                            print(f"Обработка файла: {file_path}")
                            
                            # Обрабатываем файл и сохраняем результаты
                            result = process_excel_file(file_path)
                            result['country'] = country
                            result['year'] = year
                            results.append(result)

    # Преобразуем результаты в DataFrame и сохраняем в Excel
    df_results = pd.DataFrame(results)
    df_results.to_excel('aggregated_results.xlsx', index=False)

    print("Все результаты сохранены в aggregated_results.xlsx")

# Укажите путь к корневой папке, где находятся страны
root_folder = r'C:\Users\BG\Desktop\СРСП week 6. Иванников Артём. БИ2209'
process_all_files(root_folder)
