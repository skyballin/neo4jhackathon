"""
Neo4J Hackathon Project: City Schools

Description: This project incorporates a neo4J graph database to populate a graph
from some publically available data that shows the relationships between county's,
immunization records from school, school drop out rates, and school scores. The 
entire program and the data collection were all done within 2.5 hours. 

Award: Best Use Of Cypher

Participants: Ajay, Robert, Chris, Daniel
"""


import pickle
import py2neo
from py2neo import Node, Relationship, authenticare, Graph 
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient.query import Q

city = pickle.load(open('city.pkl', 'rb'))
county = pickle.load(open('county.pkl','rb'))
query = "MATCH (n:MMR) RETURN n LIMIT 1"

username = 'neo4j' #default
password = 'neo4j' #default

graph = Graph('http://%s:%s@localhost:7474/db/data/' % (username, password))

#Create labels for all immunization records
countys = gdb.labels.create("County")
uptodate = gdb.labels.create("up-to-date")
condit = gdb.labels.create("conditional")
mmr = gdb.labels.create('MMR')
polio = gdb.labels.create('POLIO')

#Populate the database with county immunization rate nodes and relationships
for i,r in county.iterrows():
    cty = Node('County', name=r['index'])
    td = graph.find_one('todate', 'value', r['UP-TO-DATE%'])
    if not td:
        td = Node('todate', value = r['UP-TO-DATE%'])
    rel = Relationship(cty, 'is', td)
    graph.create(rel)

    cond = graph.find_one('condition', 'value', r['CONDI-TIONAL%'])
    if not cond:
        cond = Node('condition', value = r['CONDI-TIONAL%'])
    rel = Relationship(cty, 'is2', cond)
    graph.create(rel)

    mr = graph.find_one('mrrr', 'value', r['1+ MMR%'])
    if not mr:
        mr = Node('mrrr', value = r['1+ MMR%'])
    rel = Relationship(cty, 'is3', mr)
    graph.create(rel)

    polio = graph.find_one('polio', 'value', r['3+POLIO%'])
    if not polio:
        polio = Node('polio', value = r['3+POLIO%'])
    rel = Relationship(cty, 'is4', polio)
    graph.create(rel)
    
    do = graph.find_one('dropout', 'value', r['dropout_rate'])
    if not do:
        do = Node('dropout', value = r['dropout_rate'])
    rel = Relationship(cty, 'is5', do)
    graph.create(rel)

#Populate the database with schools, test scores, and relate counties. 
for i,r in city.iterrows():
    school = Node('School', name=''.join([ch for ch in i if ord(ch)<128]))

    cot = graph.find_one('County', 'name', r['county'].upper())
    if not cot:
        cot = Node('County', value = r['county'].upper())
    rel = Relationship(school, 'in', cot)
    graph.create(rel)
    
    read = graph.find_one('Read', 'value', r['AvgScrRead'])
    if not read:
        read = Node('Read', value = r['AvgScrRead'])
    rel = Relationship(school, 'reads', read)
    graph.create(rel)

    math = graph.find_one('Math', 'value', r['AvgScrMath'])
    if not math:
        math = Node('Math', value = r['AvgScrMath'])
    rel = Relationship(school, 'maths', math)
    graph.create(rel)

    write = graph.find_one('Writes', 'value', r['AvgScrWrite'])
    if not write:
        write = Node('Writes', value = r['AvgScrWrite'])
    rel = Relationship(school, 'writes', write)
    graph.create(rel)
