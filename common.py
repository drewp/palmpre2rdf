from __future__ import division
import time, sys, datetime
from rdflib import Literal, Namespace, URIRef, RDF
from rdflib.Graph import Graph
from dateutil.tz import tzlocal
import sqlite3

XS = Namespace("http://www.w3.org/2001/XMLSchema#")
PRE = Namespace("http://bigasterisk.com/pre/general/") # URIs for all Pres

def literalFromPreTime(t):
    """this puts all times in your current time zone, which is not
    necessarily the one you were in when the event occurred (but we
    don't have that info)"""
    secs = int(t) / 1000
    s = datetime.datetime.fromtimestamp(secs, tzlocal()).isoformat()
    return Literal(s, datatype=XS.dateTime)

def literalFromPreElapsed(t):
    """i'm guessing that the pre's call duration is also integer
    milliseconds"""
    return Literal(int(t) / 1000, datatype=PRE.second)

def rdfBool(b):
    if b:
        return Literal('true', datatype=XS.boolean)
    else:
        return Literal('false', datatype=XS.boolean)

def makeOutputGraph():
    graph = Graph()
    graph.bind('pre', 'http://bigasterisk.com/pre/general/')
    graph.bind('local', 'http://bigasterisk.com/pre/drew/') # todo
    graph.bind('ad', 'http://bigasterisk.com/pre/general/accountDataType/')
    graph.bind('mt', 'http://bigasterisk.com/pre/general/messageType/')
    return graph

class PalmDb(object):
    def __init__(self, dbFilename, phoneIdKeyword):
        """
        dbFilename is the path to a PalmDatabase.db3 file, which comes
        from /var/luna/data/dbdata on a pre.

        phoneIdKeyword will be used in URIs along with ID numbers from
        the phone to unique the same number between two phones
        """
        self.conn = sqlite3.connect(dbFilename)
        self.conn.row_factory=sqlite3.Row
        self.cur = self.conn.cursor()
        self.phoneIdKeyword = phoneIdKeyword

    def phoneUri(self, id):
        """uri for an id on this phone"""
        if str(id) == '-1':
            raise ValueError("invalid id")
        # if these had a non-number to start with, they would
        # abbreviate better in n3
        return URIRef("http://bigasterisk.com/pre/%s/%s" %
                      (self.phoneIdKeyword, id))

    def execute(self, *args):
        return self.cur.execute(*args)
    
    def classUri(self, row):
        """row is a db row with a _class_id column in it"""
        cur2 = self.conn.cursor()
        rows = list(cur2.execute(
            "SELECT className FROM _Class WHERE id = ?", (row['_class_id'],)))
        assert len(rows) == 1
        return PRE['class/%s' % rows[0]['className']]

    def addStatementsFromRows(self, graph, tableName,
                              simpleLiteralCols=[], convertLiteralCols=[],
                              linkCols=[], enumCols=[], rowHooks=[]):
        """
        simpleLiteralCols are strings where the column name is the
        predicate name and the object is just the sqlite cell as a
        literal

        convertLiteralCols are pairs of (columnName,
        convertorFunc). The converter must return an rdflib node type

        linkCols are names of columns where the value is an id on this
        phone. We'll remove an _id or Id suffix from the column name
        to make the predicate name.

        enumCols are a list of (columnName, pred, valueNamespace). We
        make triples with the given predicate, where the object is a
        URIRef made of valueNamespace plus the string value in the
        sqlite cell

        rowHooks is a list of functions that will be called with
        (graph, subj, row) so you can add your own statements
        """
        for row in self.cur.execute("SELECT * FROM %s" % tableName): # ? not allowed
            uri = self.phoneUri(row['id'])

            try:
                graph.add((uri, RDF.type, self.classUri(row)))
            except IndexError:
                print >>sys.stderr, "unknown class in row %s, skipping" % row

            for col in simpleLiteralCols:
                if row[col] is not None:
                    graph.add((uri, PRE[col], Literal(row[col])))

            for col, conv in convertLiteralCols:
                if row[col] is not None:
                    graph.add((uri, PRE[col], conv(row[col])))

            for col in linkCols:
                try:
                    obj = self.phoneUri(row[col])
                except ValueError:
                    continue
                pred = col
                if pred.endswith('_id'):
                    pred = pred[:-3]
                if pred.endswith('Id'):
                    pred = pred[:-2]
                graph.add((uri, PRE[pred], obj))

            for col, pred, valueNs in enumCols:
                if row[col] is not None:
                    graph.add((uri, pred, URIRef(valueNs + row[col])))

            for func in rowHooks:
                func(graph, uri, row)
