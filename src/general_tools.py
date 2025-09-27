#Funciones generales para el proyecto IBD Reddit

import pandas as pd
import numpy as np
import re
import os
from datetime import datetime
import time
import ast

def process_file(comments_path, submissions_path, out_csv):
    """Versión final que evita problemas con apply sobre arrays."""
    import pandas as pd
    import time
    
    start_time = time.time()
    
    print("Cargando datos...")
    cd_comments = pd.read_csv(comments_path)
    cd_submissions = pd.read_csv(submissions_path)
    
    print(f"Datos: {len(cd_comments):,} comentarios, {len(cd_submissions):,} submissions")
    
    # Limpiar prefijo t3_
    print("Limpiando prefijos...")
    cd_comments['link_id_clean'] = cd_comments['link_id'].str.replace('t3_', '', regex=False)
    
    # MÉTODO ALTERNATIVO: Usar diccionario para evitar problemas con merge
    print(" Creando diccionario de comentarios...")
    comments_dict = {}
    
    # Agrupar comentarios manualmente
    for idx, row in cd_comments.iterrows():
        post_id = row['link_id_clean']
        comment_body = row['body']
        
        if post_id not in comments_dict:
            comments_dict[post_id] = []
        comments_dict[post_id].append(comment_body)
        
        # Mostrar progreso cada 100k comentarios
        if idx % 100000 == 0:
            print(f"   Procesado {idx:,} comentarios...")
    
    print(f"Diccionario creado con {len(comments_dict):,} posts únicos")
    
    # Asignar comentarios a submissions usando el diccionario
    print(" Asignando comentarios a submissions...")
    
    # Crear listas vacías para todos los posts primero
    comments_list = []
    comment_counts = []
    
    for idx, row in cd_submissions.iterrows():
        post_id = row['id']
        post_comments = comments_dict.get(post_id, [])  # Lista vacía si no hay comentarios
        
        comments_list.append(post_comments)
        comment_counts.append(len(post_comments))
        
        # Mostrar progreso cada 10k submissions
        if idx % 10000 == 0:
            print(f"   Procesado {idx:,} submissions...")
    
    # Asignar las listas directamente sin usar apply
    cd_submissions['comments'] = comments_list
    cd_submissions['comment_count'] = comment_counts
    
    # Guardar resultado
    print(f"Guardando en {out_csv}...")
    cd_submissions.to_csv(out_csv, index=False)
    
    # Estadísticas finales
    total_comments = sum(comment_counts)
    posts_with_comments = sum(1 for count in comment_counts if count > 0)
    
    elapsed_time = time.time() - start_time
    
    print(f"\n¡PROCESO COMPLETADO EN {elapsed_time:.2f} SEGUNDOS!")
    print(f"   Total submissions: {len(cd_submissions):,}")
    print(f"   Total comentarios unidos: {total_comments:,}")
    print(f"   Posts con comentarios: {posts_with_comments:,}")
    print(f"   Posts sin comentarios: {len(cd_submissions) - posts_with_comments:,}")
    
    return cd_submissions


def combine_ibd_datasets(data_paths, output_path='../data/processed/IBD_complete_dataset.csv'):
    """
    Combina múltiples datasets de IBD en un solo dataframe.
    
    Parameters:
    -----------
    data_paths : dict
        Diccionario con nombres de datasets como keys y rutas de archivos como values
    output_path : str
        Ruta donde guardar el dataset combinado
    
    Returns:
    --------
    pd.DataFrame
        Dataset combinado con todos los datos
    """
    import pandas as pd
    import time
    
    start_time = time.time()
    
    print("Iniciando combinación de datasets IBD...")
    print(f"Datasets a procesar: {len(data_paths)}")
    
    # Cargar todos los dataframes
    dataframes = {}
    total_rows = 0
    
    print("\nCargando dataframes...")
    for name, path in data_paths.items():
        try:
            df = pd.read_csv(path)
            dataframes[name] = df
            total_rows += len(df)
            print(f"   {name}: {len(df):,} filas")
        except Exception as e:
            print(f"   {name}: Error al cargar - {e}")
    
    print(f"\nTotal filas a combinar: {total_rows:,}")
    
    # Renombrar columnas en dataframes "_1000"
    print("\nRenombrando columnas 'author_comments' a 'comments'...")
    
    for name, df in dataframes.items():
        if '_1000' in name or 'last1000' in name:
            if 'author_comments' in df.columns:
                df.rename(columns={'author_comments': 'comments'}, inplace=True)
                print(f"   {name}: Renombrado 'author_comments' a 'comments'")
            elif 'comments' in df.columns:
                print(f"   {name}: Ya tiene columna 'comments'")
            else:
                print(f"    {name}: No tiene 'author_comments' ni 'comments'")
    
    # Verificar compatibilidad de columnas
    print("\n🔍 Verificando compatibilidad de columnas...")
    all_columns = set()
    for name, df in dataframes.items():
        all_columns.update(df.columns)
    
    print(f"   Total columnas únicas encontradas: {len(all_columns)}")
    
    # Concatenar dataframes
    print("\n🔧 Concatenando dataframes...")
    try:
        df_list = list(dataframes.values())
        combined_df = pd.concat(df_list, ignore_index=True, sort=False)
        
        print(f"Concatenación exitosa!")
        print(f"   Total filas: {len(combined_df):,}")
        print(f"    Total columnas: {combined_df.shape[1]}")
        
        # Estadísticas por subreddit
        if 'subreddit' in combined_df.columns:
            print("\nDistribución por subreddit:")
            subreddit_counts = combined_df['subreddit'].value_counts()
            for subreddit, count in subreddit_counts.items():
                print(f"   {subreddit}: {count:,} posts")
        
        # Verificar columnas con muchos NaN
        print("\nAnálisis de valores faltantes:")
        nan_counts = combined_df.isnull().sum()
        high_nan_cols = nan_counts[nan_counts > len(combined_df) * 0.5]
        
        if len(high_nan_cols) > 0:
            print("   Columnas con >50% valores faltantes:")
            for col, count in high_nan_cols.items():
                percentage = (count / len(combined_df)) * 100
                print(f"     {col}: {count:,} ({percentage:.1f}%)")
        else:
            print("   Ninguna columna tiene >50% valores faltantes")
        
        # Guardar resultado
        print(f"\nGuardando dataset combinado...")
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        combined_df.to_csv(output_path, index=False)
        
        elapsed_time = time.time() - start_time
        print(f"\n¡PROCESO COMPLETADO EN {elapsed_time:.2f} SEGUNDOS!")
        print(f"   Archivo guardado: {output_path}")
        print(f"   Dataset final: {len(combined_df):,} filas × {combined_df.shape[1]} columnas")
        
        return combined_df
        
    except Exception as e:
        print(f"Error durante la concatenación: {e}")
        return None




def preprocess_text_columns(df, output_path='../data/interim/IBD_estructured_text.csv'):
    """
    Preprocesa las columnas de texto con formato estructurado y saltos de línea.
    
    Formato de salida:
    TÍTULO:
    [título del post]
    
    CONTENIDO:
    [selftext]
    
    COMENTARIOS:
    - [comentario 1]
    - [comentario 2]
    - [comentario 3]
    """
    
    def process_comments_column(comments_value):
        """Procesa la columna comments que puede ser string, lista, o NaN."""
        if pd.isna(comments_value) or comments_value == '':
            return []
        
        if isinstance(comments_value, list):
            return [str(comment).strip() for comment in comments_value if pd.notna(comment) and str(comment).strip()]
        
        if isinstance(comments_value, str):
            if comments_value.strip().startswith('['):
                try:
                    comment_list = ast.literal_eval(comments_value)
                    if isinstance(comment_list, list):
                        return [str(comment).strip() for comment in comment_list if pd.notna(comment) and str(comment).strip()]
                except:
                    return [comments_value.strip()]
            else:
                return [comments_value.strip()]
        
        return []
    
    def clean_text(text):
        """Limpia y normaliza texto."""
        if pd.isna(text) or text == '' or str(text).lower() in ['nan', 'none', 'missing value']:
            return ''
        return str(text).strip()
    
    def combine_text_fields(row):
        """Combina title, selftext y comments con formato estructurado."""
        
        title = clean_text(row['title'])
        selftext = clean_text(row['selftext'])
        comments_list = process_comments_column(row['comments'])
        
        combined_parts = []
        
        # Agregar título
        if title:
            combined_parts.append(f"TÍTULO:\n{title}")
        
        # Agregar contenido
        if selftext:
            combined_parts.append(f"CONTENIDO:\n{selftext}")
        
        # Agregar comentarios con viñetas
        if comments_list:
            comments_formatted = []
            for comment in comments_list:
                # Limitar longitud para evitar comentarios muy largos
                comment_clean = comment[:500] + "..." if len(comment) > 500 else comment
                comments_formatted.append(f"- {comment_clean}")
            
            comments_section = f"COMENTARIOS:\n" + "\n".join(comments_formatted)
            combined_parts.append(comments_section)
        
        # Unir con doble salto de línea
        return '\n\n'.join(combined_parts)
    
    print("Procesando columnas de texto con formato estructurado...")
    
    df_processed = df.copy()
    df_processed['cuerpo'] = df_processed.apply(combine_text_fields, axis=1)
    
    # Estadísticas
    total_rows = len(df_processed)
    non_empty_cuerpo = (df_processed['cuerpo'] != '').sum()
    
    # Guardar resultado
    print(f"\nGuardando dataset estructurado...")
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_processed.to_csv(output_path, index=False)

    print(f"Procesamiento completado:")
    print(f"   Total filas: {total_rows:,}")
    print(f"   Filas con contenido: {non_empty_cuerpo:,}")
    print(f"   Archivo guardado: {output_path}")
    
    return df_processed
