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

# Declara Import
import os
import csv
import sqlite3
import json
import requests
import re

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Declara variables Globales
artics = []

# Crear el motor (engine) de la base de datos
engine = sqlalchemy.create_engine("sqlite:///mercadolibre.db")
base = declarative_base()

from config import config

# Obtener la path de ejecución actual del script
script_path = os.path.dirname(os.path.realpath(__file__))

# Obtener los parámetros del archivo de configuración
config_path_name = os.path.join(script_path, 'config.ini')
dataset = config('dataset', config_path_name)

class Articulos(base):
    __tablename__ = "articulos"
    id = Column(String, primary_key=True, autoincrement=False)
    site_id = Column(String)
    title = Column(String)
    price = Column(Integer)
    currency_id = Column(String)
    initial_quantity = Column(Integer)
    available_quantity = Column(Integer)
    sold_quantity = Column(Integer)
    
    def __repr__(self):
        return f"Articulos: {self.site_id}, nombre {self.title}, grado {self.price}, tutor {self.currency_id}, Cantidad_Inicial {self.initial_quantity}, Cantidad_Disponible {self.available_quantity}, Cantidad_Vendida{self.sold_quantity} "



def create_schema():
    # Borrar todos las tablas existentes en la base de datos
    # Esta linea puede comentarse sino se eliminar los datos
    base.metadata.drop_all(engine)

    # Crear las tablas
    base.metadata.create_all(engine)

def carga_items_dataBase(data):
    artics.clear
    posicion = 0

    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Carga datos a Tabla 
    articulos = Articulos(id = data['id'], 
                          site_id = data['site_id'],
                          title = data['title'],
                          price = data['price'],
                          currency_id = data['currency_id'],
                          initial_quantity = data['initial_quantity'],
                          available_quantity = data['available_quantity'],
                          sold_quantity = data['sold_quantity'])
        
    # Agrega articulo a Data Base
    session.add(articulos)
    session.commit()   
    return



def invoca_api(url):
    # Recibe una URL y recupera un objeto json_response (Tipo Dataset)
    try:
        response = requests.get(url)
        dataset = response.json()  

        # recupera dataset
        json_response = dataset[0]["body"]  
        carga_items_dataBase(json_response)
    except:
        return     


def insert_articulos():
    # Insertar el archivo CSV de meli_technical
    # Insertar todas las filas juntas
    with open('meli_technical_challenge_data.csv', 'r') as f:
        data = list(csv.DictReader(f))
  
        # Ciclo , invoca API 
        for row in data:
            # Arma Id (Site + Id)
            id = row['site'] + row['id']
            # Arma URL dinamica 
            url = 'https://api.mercadolibre.com/items?ids={}'.format(id)
            # Invoca API 
            invoca_api(url)   
 

def fill():
    print('Completemos esta tablita!')
 
    # Llenar la tabla de articulos
    # Cada articulos tiene los posibles campos:
    # id  -----------------> Este campo corresponde al codigo de articulo
    # site_id -------------> Este campo corresponde a la ubicacion
    # title ---------------> Este campo corresponde a la descripcion
    # price ---------------> Este campo corresponde al precio
    # currency_id ---------> Este campo corresponde al codigo de moneda
    # initial_quantity ----> Este campo corresponde a la cantidad inicial
    # available_quantity --> Este campo corresponde a la cantidad disponible
    # sold_quantity -------> Este campo corresponde a la cantidad vendida
    insert_articulos()


def fetch(id):
    print('Comprovemos su contenido, ¿qué hay en la tabla?')
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()
    # Crear una query para imprimir en pantalla
    query = session.query(Articulos).filter(Articulos.id == id)
    # todos los objetos creados de la tabla estudiante.
    # Imprimir en pantalla cada objeto que traiga la query
    # Realizar un bucle para imprimir de una fila a la vez
    for articulo in query:
        print(articulo)


if __name__ == '__main__':
    print("Bienvenidos a otra clase de Inove con Python")
    create_schema()   # create and reset database (DB)
    fill()            # carga data base - CSV + API
  