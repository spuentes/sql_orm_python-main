#!/usr/bin/env python
'''
SQL Introducción [Python]
Ejercicios de práctica
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa creado para poner a prueba los conocimientos
adquiridos durante la clase
'''

__author__ = "Inove Coding School"
__email__ = "alumnos@inove.com.ar"
__version__ = "1.1"

import os
import sqlite3

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Crear el motor (engine) de la base de datos
engine = sqlalchemy.create_engine("sqlite:///secundaria.db")
base = declarative_base()

from config import config

# Obtener la path de ejecución actual del script
script_path = os.path.dirname(os.path.realpath(__file__))

# Obtener los parámetros del archivo de configuración
config_path_name = os.path.join(script_path, 'config.ini')
dataset = config('dataset', config_path_name)

class Tutor(base):
    __tablename__ = "tutor"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    
    def __repr__(self):
        return f"Tutor: {self.name}"


class Estudiante(base):
    __tablename__ = "estudiante"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
    grade = Column(Integer)
    tutor_id = Column(Integer, ForeignKey("tutor.id"))

    tutor = relationship("Tutor")

    def __repr__(self):
        return f"Estudiante: {self.name}, edad {self.age}, grado {self.grade}, tutor {self.tutor.name}"


def create_schema():
    # Borrar todos las tablas existentes en la base de datos
    # Esta linea puede comentarse sino se eliminar los datos
    base.metadata.drop_all(engine)

    # Crear las tablas
    base.metadata.create_all(engine)


def insert_tutor():
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    list_tutor = ['Juan Soto',
                  'Pedro Gonzalez',
                  'Miguel Perez'
                 ]
    # Bucle list tutor
    for nombre in list_tutor:            
        # Crear una nueva nacionalidad
        tutor = Tutor(name = nombre)

        # Agregar la nacionalidad a la DB
        session.add(tutor)
        session.commit()

    # Visualiza tabla tutor    
    query = session.query(Tutor)
    # Leer una persona a la vez e imprimir en pantalla
    for tutor in query:
        print(tutor)


def insert_estudiante():
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    estudiantes = [('Marcos Lopetegui', 10, 5, 'Juan Soto'),
                   ('Javier Rocco', 13, 7, 'Miguel Perez'),
                   ('Marcela Garcia', 12, 6, 'Pedro Gonzalez'),
                   ('Patricia Ramirez', 13, 7, 'Miguel Perez'),
                   ('Tadeo Maggi', 14, 8, 'Pedro Gonzalez') 
                  ]

    # Bucle list tutor
    for alumno in estudiantes:     
        query = session.query(Tutor).filter(Tutor.name == alumno[3])
        tutor = query.first()
       
        # Crear una nuevo estudiante
        estudiante = Estudiante(name = alumno[0], age = alumno[1], grade = alumno[2])
        estudiante.tutor_id = tutor.id

        # Agregar la nacionalidad a la DB
        session.add(estudiante)
        session.commit()

     # Visualiza tabla tutor    
    query = session.query(Estudiante)
    # Leer una persona a la vez e imprimir en pantalla
    for estudiante in query:
        print(estudiante)


def fill():
    print('Completemos esta tablita!')
    # Llenar la tabla de la secundaria con al menos 2 tutores
    # Cada tutor tiene los campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del tutor (puede ser solo nombre sin apellido)
    insert_tutor()
    
    # Llenar la tabla de la secundaria con al menos 5 estudiantes
    # Cada estudiante tiene los posibles campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del estudiante (puede ser solo nombre sin apellido)
    # age --> cuantos años tiene el estudiante
    # grade --> en que año de la secundaria se encuentra (1-6)
    # tutor --> el tutor de ese estudiante (el objeto creado antes)
    insert_estudiante()

    # No olvidarse que antes de poder crear un estudiante debe haberse
    # primero creado el tutor.


def fetch():
    print('Comprovemos su contenido, ¿qué hay en la tabla?')
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()
    # Crear una query para imprimir en pantalla
    query = session.query(Estudiante)
    # todos los objetos creados de la tabla estudiante.
    # Imprimir en pantalla cada objeto que traiga la query
    # Realizar un bucle para imprimir de una fila a la vez
    for estudiante in query:
        print(estudiante)


def search_by_tutor(tutor):
    print('Operación búsqueda!')
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Esta función recibe como parámetro el nombre de un posible tutor.
    # Crear una query para imprimir en pantalla
    # aquellos estudiantes que tengan asignado dicho tutor.
    query = session.query(Tutor).filter(Tutor.name == tutor)

    # Para poder realizar esta query debe usar join, ya que
    # deberá crear la query para la tabla estudiante pero
    # buscar por la propiedad de tutor.name
    
    # Leer una persona a la vez e imprimir en pantalla
    for tutor in query:
        print(tutor)

def modify(id, name):
    print('Modificando la tabla')
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Deberá actualizar el tutor de un estudiante, cambiarlo para eso debe
    # 1) buscar con una query el tutor por "tutor.name" usando name
    # Buscar la nacionalidada que se desea actualizar
    rowcount = session.query(Tutor).filter(Tutor.name == name).count()
    if (rowcount > 0):
        query = session.query(Tutor).filter(Tutor.name == name)
        tutor = query.first()

    # Buscar la persona que se desea actualizar
    # pasado como parámetro y obtener el objeto del tutor
    # 2) buscar con una query el estudiante por "estudiante.id" usando
    # el id pasado como parámetro
        rowcount = session.query(Estudiante).filter(Estudiante.id == id).count()
        if (rowcount > 0):
            query = session.query(Estudiante).filter(Estudiante.id == id)
            estudiante = query.first() 

    # 3) actualizar el objeto de tutor del estudiante con el obtenido
    # en el punto 1 y actualizar la base de datos
               
            session.query(Estudiante).filter(Estudiante.id == id).update({Estudiante.tutor_id: tutor.id})
	
    # Aunque la persona ya existe, como el id coincide
    # se actualiza sin generar una nueva entrada en la DB
            session.commit()

            # Recupera datos actualizados
            query = session.query(Estudiante).filter(Estudiante.id == id)
            for estudiante in query:
                print(estudiante)
        else:
            print('No existe Estudiante con Id', id)                    
    else:
        print('No existe Tutor de Nombre', name)           

    

def count_grade(grade):
    print('Estudiante por grado')
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Utilizar la sentencia COUNT para contar cuantos estudiante
    # se encuentran cursando el grado "grade" pasado como parámetro
    rowcount = session.query(Estudiante).filter(Estudiante.grade == grade).count()
 
 
     # Verifica si, existe estudiante del grado a actualizar
    if (rowcount > 0):  
        session.query(Estudiante).update({Estudiante.grade: grade})
        session.commit()
    
        # Imprimir en pantalla el resultado
        print('Grados actualizada', grade. rowcount)
    else:
       print('No Existen Estudiantes con grado', grade)               


if __name__ == '__main__':
    print("Bienvenidos a otra clase de Inove con Python")
    create_schema()   # create and reset database (DB)
    fill()
    fetch()

    tutor = 'Juan Soto'
    search_by_tutor(tutor)

    nuevo_tutor = 'Pedro Gonzalez'
    id = 2
    modify(id, nuevo_tutor)

    grade = 2
    count_grade(grade)
