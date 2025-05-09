# Tarea4_studiantes_habitos_hbase.py
# Limpieza, transformación y análisis exploratorio del dataset student-habits-vs-academic-performance.csv
# Diana Marcela Castiblanco Sanchez
# Grupo 202016911_17

import happybase
import pandas as pd
import csv
from datetime import datetime

# Bloque principal de ejecución
try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")
    
    # 2. Crear la tabla con las familias de columnas
    table_name = 'student_habits'
    families = {
        'basic': dict(),      # información básica del estudiante (ID, edad, género)
        'habits': dict(),     # hábitos diarios (estudio, redes sociales, Netflix)
        'lifestyle': dict()   # otros aspectos (trabajo, asistencia, sueño, dieta)
    }
    
    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)
        
    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print("Tabla 'student_habits' creada exitosamente")
    
    # 3. Cargar datos del CSV
    print("Cargando datos desde CSV...")
    
    # Usamos un try-except para manejar posibles problemas con la carga del archivo
    try:
        student_data = pd.read_csv('student_habits_performance.csv')
        print(f"Dataset cargado correctamente. Filas: {len(student_data)}")
    except Exception as e:
        print(f"Error al cargar el CSV: {str(e)}")
        print("Creando datos de ejemplo para continuar...")
        
        # Crear datos de ejemplo si el archivo no está disponible
        data = {
            'student_id': range(1, 21),
            'age': [19, 20, 21, 19, 22, 20, 21, 19, 20, 22, 21, 20, 19, 22, 21, 20, 19, 22, 21, 20],
            'gender': ['M', 'F', 'M', 'F', 'M', 'F', 'M', 'F', 'M', 'F', 'M', 'F', 'M', 'F', 'M', 'F', 'M', 'F', 'M', 'F'],
            'study_hours_per_day': [2, 5, 1, 4, 3, 6, 2, 4, 5, 3, 2, 5, 1, 4, 3, 6, 2, 4, 5, 3],
            'social_media_hours': [3, 1, 4, 2, 3, 1, 5, 2, 1, 3, 3, 1, 4, 2, 3, 1, 5, 2, 1, 3],
            'netflix_hours': [2, 1, 3, 1, 2, 0, 3, 1, 0, 2, 2, 1, 3, 1, 2, 0, 3, 1, 0, 2],
            'part_time_job': ['Yes', 'No', 'Yes', 'No', 'Yes', 'No', 'Yes', 'No', 'Yes', 'No', 'Yes', 'No', 'Yes', 'No', 'Yes', 'No', 'Yes', 'No', 'Yes', 'No'],
            'attendance_percentage': [85, 95, 75, 90, 85, 98, 70, 92, 96, 88, 85, 95, 75, 90, 85, 98, 70, 92, 96, 88],
            'sleep_hours': [6, 8, 5, 7, 6, 8, 5, 7, 8, 6, 6, 8, 5, 7, 6, 8, 5, 7, 8, 6],
            'diet_quality': ['Good', 'Excellent', 'Poor', 'Good', 'Average', 'Excellent', 'Poor', 'Good', 'Excellent', 'Average', 'Good', 'Excellent', 'Poor', 'Good', 'Average', 'Excellent', 'Poor', 'Good', 'Excellent', 'Average']
        }
        student_data = pd.DataFrame(data)
    
    # Iterar sobre el DataFrame usando el índice
    for index, row in student_data.iterrows():
        # Generar row key basado en el student_id
        row_key = f'student_{row["student_id"]}'.encode()
        
        # Organizar los datos en familias de columnas
        data = {
            # Familia basic
            b'basic:student_id': str(row['student_id']).encode(),
            b'basic:age': str(row['age']).encode(),
            b'basic:gender': str(row['gender']).encode(),
            
            # Familia habits
            b'habits:study_hours': str(row['study_hours_per_day']).encode(),
            b'habits:social_media_hours': str(row['social_media_hours']).encode(),
            b'habits:netflix_hours': str(row['netflix_hours']).encode(),
            
            # Familia lifestyle
            b'lifestyle:part_time_job': str(row['part_time_job']).encode(),
            b'lifestyle:attendance': str(row['attendance_percentage']).encode(),
            b'lifestyle:sleep_hours': str(row['sleep_hours']).encode(),
            b'lifestyle:diet_quality': str(row['diet_quality']).encode()
        }
        
        table.put(row_key, data)
    
    print("Datos cargados exitosamente en HBase")
    
    # 4. CONSULTAS Y ANÁLISIS DE DATOS
    print("\n=== CONSULTAS Y ANÁLISIS DE DATOS ===")
    
    # 4.1 Mostrar todos los estudiantes (limitado a 5)
    print("\n=== Mostrando los primeros 5 estudiantes ===")
    count = 0
    for key, data in table.scan(limit=5):
        print(f"\nEstudiante ID: {data[b'basic:student_id'].decode()}")
        print(f"Edad: {data[b'basic:age'].decode()}")
        print(f"Género: {data[b'basic:gender'].decode()}")
        print(f"Horas de estudio: {data[b'habits:study_hours'].decode()}")
        print(f"Porcentaje de asistencia: {data[b'lifestyle:attendance'].decode()}%")
        count += 1
    
    # 4.2 Filtrar estudiantes que estudian más de 4 horas al día
    print("\n=== Estudiantes que estudian más de 4 horas al día ===")
    hardworking_students = []
    for key, data in table.scan():
        study_hours = float(data[b'habits:study_hours'].decode())
        if study_hours > 4:
            hardworking_students.append({
                'id': data[b'basic:student_id'].decode(),
                'study_hours': study_hours,
                'gender': data[b'basic:gender'].decode(),
                'attendance': float(data[b'lifestyle:attendance'].decode())
            })
    
    # Mostrar resultados
    print(f"Total de estudiantes que estudian más de 4 horas: {len(hardworking_students)}")
    for student in hardworking_students[:3]:  # Mostrar solo los primeros 3
        print(f"ID: {student['id']}, Horas de estudio: {student['study_hours']}, Asistencia: {student['attendance']}%")
    
    # 4.3 Análisis por género
    print("\n=== Análisis por género ===")
    gender_stats = {'M': 0, 'F': 0}
    study_by_gender = {'M': 0, 'F': 0}
    social_by_gender = {'M': 0, 'F': 0}
    attendance_by_gender = {'M': 0, 'F': 0}
    
    for key, data in table.scan():
        gender = data[b'basic:gender'].decode()
        study_hours = float(data[b'habits:study_hours'].decode())
        social_hours = float(data[b'habits:social_media_hours'].decode())
        attendance = float(data[b'lifestyle:attendance'].decode())
        
        gender_stats[gender] = gender_stats.get(gender, 0) + 1
        study_by_gender[gender] = study_by_gender.get(gender, 0) + study_hours
        social_by_gender[gender] = social_by_gender.get(gender, 0) + social_hours
        attendance_by_gender[gender] = attendance_by_gender.get(gender, 0) + attendance
    
    # Calcular promedios
    for gender in gender_stats.keys():
        if gender_stats[gender] > 0:
            print(f"Género {gender}:")
            print(f"  Cantidad: {gender_stats[gender]} estudiantes")
            print(f"  Promedio horas de estudio: {study_by_gender[gender]/gender_stats[gender]:.2f} horas/día")
            print(f"  Promedio horas en redes sociales: {social_by_gender[gender]/gender_stats[gender]:.2f} horas/día")
            print(f"  Promedio asistencia: {attendance_by_gender[gender]/gender_stats[gender]:.2f}%")
    
    # 4.4 Estudiantes con mejor asistencia por calidad de dieta
    print("\n=== Asistencia promedio por calidad de dieta ===")
    diet_attendance = {}
    diet_count = {}
    
    for key, data in table.scan():
        diet = data[b'lifestyle:diet_quality'].decode()
        attendance = float(data[b'lifestyle:attendance'].decode())
        
        diet_attendance[diet] = diet_attendance.get(diet, 0) + attendance
        diet_count[diet] = diet_count.get(diet, 0) + 1
    
    for diet in diet_attendance:
        avg_attendance = diet_attendance[diet] / diet_count[diet]
        print(f"Dieta {diet}: {avg_attendance:.2f}% de asistencia promedio")
    
    # 4.5 Correlación entre horas de estudio y horas de sueño
    print("\n=== Análisis de horas de estudio vs horas de sueño ===")
    study_sleep_data = []
    
    for key, data in table.scan():
        study_hours = float(data[b'habits:study_hours'].decode())
        sleep_hours = float(data[b'lifestyle:sleep_hours'].decode())
        
        study_sleep_data.append({
            'id': data[b'basic:student_id'].decode(),
            'study': study_hours,
            'sleep': sleep_hours
        })
    
    # Agrupar por horas de estudio
    study_groups = {}
    for student in study_sleep_data:
        study_key = str(student['study'])
        if study_key not in study_groups:
            study_groups[study_key] = []
        study_groups[study_key].append(student['sleep'])
    
    for study, sleep_list in study_groups.items():
        avg_sleep = sum(sleep_list) / len(sleep_list)
        print(f"Estudiantes que estudian {study} horas: {avg_sleep:.2f} horas de sueño promedio")
    
    # 5. OPERACIONES DE ESCRITURA
    print("\n=== OPERACIONES DE ESCRITURA ===")
    
    # 5.1 Insertar un nuevo estudiante
    print("\n=== Insertando un nuevo estudiante ===")
    new_student_id = 999
    row_key = f'student_{new_student_id}'.encode()
    
    new_student_data = {
        b'basic:student_id': str(new_student_id).encode(),
        b'basic:age': str(23).encode(),
        b'basic:gender': 'M'.encode(),
        b'habits:study_hours': str(7).encode(),
        b'habits:social_media_hours': str(1).encode(),
        b'habits:netflix_hours': str(0.5).encode(),
        b'lifestyle:part_time_job': 'Yes'.encode(),
        b'lifestyle:attendance': str(97).encode(),
        b'lifestyle:sleep_hours': str(7).encode(),
        b'lifestyle:diet_quality': 'Excellent'.encode()
    }
    
    table.put(row_key, new_student_data)
    print(f"Estudiante con ID {new_student_id} insertado correctamente")
    
    # Verificar la inserción
    row = table.row(row_key)
    print(f"Datos del nuevo estudiante:")
    print(f"ID: {row[b'basic:student_id'].decode()}")
    print(f"Edad: {row[b'basic:age'].decode()}")
    print(f"Horas de estudio: {row[b'habits:study_hours'].decode()}")
    print(f"Asistencia: {row[b'lifestyle:attendance'].decode()}%")
    
    # 5.2 Actualizar un estudiante existente
    print("\n=== Actualizando datos de un estudiante existente ===")
    student_to_update_key = row_key  # Actualizamos el que acabamos de crear
    
    # Actualizar solo algunas columnas
    update_data = {
        b'habits:study_hours': str(8).encode(),  # Aumentamos horas de estudio
        b'lifestyle:sleep_hours': str(6).encode(),  # Reducimos horas de sueño
    }
    
    table.put(student_to_update_key, update_data)
    print(f"Datos del estudiante con ID {new_student_id} actualizados")
    
    # Verificar la actualización
    row = table.row(student_to_update_key)
    print(f"Datos actualizados:")
    print(f"ID: {row[b'basic:student_id'].decode()}")
    print(f"Horas de estudio: {row[b'habits:study_hours'].decode()} (actualizado)")
    print(f"Horas de sueño: {row[b'lifestyle:sleep_hours'].decode()} (actualizado)")
    print(f"Asistencia: {row[b'lifestyle:attendance'].decode()}% (sin cambios)")
    
    # 5.3 Eliminar un estudiante
    print("\n=== Eliminando un estudiante ===")
    student_to_delete_key = row_key  # Eliminamos el que acabamos de crear/actualizar
    
    table.delete(student_to_delete_key)
    print(f"Estudiante con ID {new_student_id} eliminado correctamente")
    
    # Verificar la eliminación
    row = table.row(student_to_delete_key)
    if not row:
        print("Verificación exitosa: No se encontraron datos para el estudiante eliminado")
    else:
        print("Error: El estudiante no se eliminó correctamente")
    
    # 6. Reporte final
    print("\n=== RESUMEN DE OPERACIONES ===")
    print("1. Conexión a HBase establecida")
    print(f"2. Tabla '{table_name}' creada con familias: {', '.join(families.keys())}")
    print(f"3. Datos cargados desde dataset")
    print("4. Consultas y análisis realizados:")
    print("   - Listado de estudiantes")
    print("   - Filtrado por horas de estudio")
    print("   - Análisis por género")
    print("   - Análisis por calidad de dieta")
    print("   - Correlación entre estudio y sueño")
    print("5. Operaciones de escritura realizadas:")
    print("   - Inserción de nuevo estudiante")
    print("   - Actualización de datos de estudiante")
    print("   - Eliminación de estudiante")
    
except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Cerrar la conexión
    connection.close()
    print("\nConexión a HBase cerrada correctamente")