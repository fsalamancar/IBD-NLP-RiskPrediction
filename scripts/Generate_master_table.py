import pandas as pd
import numpy as np
import os   
import time
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from src.general_tools import *


# Joint de los r/ CD: Comentarios y Posts
comments_path = '../data/interim/CD_historical_comments.csv'
submissions_path = '../data/interim/CD_historical_submissions.csv'
output_path = '../data/interim/CD_combined_data.csv'


result_df = process_file(comments_path, submissions_path, output_path)


# Joint de los r/ UC: Comentarios y Posts
comments_path = '../data/interim/UC_historical_comments.csv'
submissions_path = '../data/interim/UC_historical_submissions.csv'
output_path = '../data/interim/UC_combined_data.csv'
result_df = process_file(comments_path, submissions_path, output_path)


# Teniendo todos los posts y comentarios Juntar todo a un unico dataset
data_paths = {
    'CD_historical': '../data/interim/CD_combined_data.csv',
    'CrohnnColitis_1000': '../data/interim/crohnsandcolitis_last1000_20250915.csv',
    'CD_1000': '../data/interim/CrohnsDisease_last1000_20250915.csv',
    'CDiet_1000': '../data/interim/CrohnsDiseaseDiet_last1000_20250915.csv',
    'IBD_historical': '../data/interim/IBD_historical_submissions.csv',
    'IBD_last1000': '../data/interim/IBD_last1000_20250915.csv',
    'IBDDiet_1000': '../data/interim/IBDDiet_last1000_20250915.csv',
    'UC_historical': '../data/interim/UC_combined_data.csv',
    'UC_1000': '../data/interim/UlcerativeColitis_last1000_20250915.csv',
    'UCdiet_1000': '../data/interim/UlcerativeColitisDiet_last1000_20250915.csv',
    'UCRDLA_1000': '../data/interim/UlcerativeColitisRDLA_last1000_20250915.csv'
}

# Combinar las tablas en un solo dataset
combined_dataset = combine_ibd_datasets(data_paths, '../data/interim/IBD_complete_dataset.csv')
IBD_master_table = pd.read_csv('../data/interim/IBD_complete_dataset.csv')
IBD_master_table = IBD_master_table.drop(columns=['score','num_comments','comment_count','post_num_comments','permalink'])


#Preprocesado de las columnas de texto
df_processed = preprocess_text_columns(IBD_master_table)
