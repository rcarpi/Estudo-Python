import MySQLdb
import sys
import re

class mySQL:
    def __init__(self):
        try:
            self.con = MySQLdb.connect( host = "localhost", user = "root", passwd = "", db = "DATABASE")
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)
            
    def __query( self , q ):
        try:
            res = self.con.cursor( MySQLdb.cursors.DictCursor )
            res.execute( q )
            return res
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)

    def __command( self , pCommand, pTables, pFields , *pOptions):
        sql = pCommand
        if type( pTables ) is list and len( pTables ) > 0:
            sql += ', '.join( pTables )
        else:
            sql += pTables
        sql += ' set '
        if type( pFields ) is list and len( pFields ) > 0:
            sql += ', '.join( pFields )
        elif type( pFields ) is str and len( pFields ) > 0:
            sql += pFields
        for opt in pOptions:
            sql += ' ' + opt
        return self.__query( sql )

    def select( self , pTables, pFields = ' * ' , *pOptions):
        sql = 'select '
        if type( pFields ) is list and len( pFields ) > 0:
            sql += ', '.join( pFields )
        elif type( pFields ) is str and len( pFields ) > 0:
            sql += pFields
        else:
            sql += ' * '
        sql += ' from '
        if type( pTables ) is list and len( pTables ) > 0:
            sql += ', '.join( pTables )
        else:
            sql += pTables
        for opt in pOptions:
            sql += ' ' + opt
        return self.__query( sql )

    def delete( self , pTables, *pOptions):
        sql = "delete from "
        if type( pTables ) is list and len( pTables ) > 0:
            sql += ', '.join( pTables )
        else:
            sql += pTables
        for opt in pOptions:
            sql += ' ' + opt
        return self.__query( sql ).rowcount 

    def insert( self , pTables, pFields , *pOptions):
        return self.__command('insert into ', pTables, pFields , *pOptions).lastrowid 

    def update( self , pTables, pFields , *pOptions):
        return  self.__command('update ', pTables, pFields , *pOptions).rowcount 
    
    def results( self , pRes ):
        obj = []
        for row in pRes.fetchall():
            obj.append( row )
        return obj

    def fields_config( self , pTables ):
        obj = {}
        res = self.__query("SHOW COLUMNS FROM " + pTables) 
        for row in self.results( res ):
            obj[ row["Field"] ] = {}
            if row["Key"] == "PRI":
                obj[ row["Field"] ][ "PK" ] = True 
            
            if row["Key"] == "UNI":
                obj[ row["Field"] ]["UNI"] = True

            r = re.match(r"(?P<TYPE>\w+)\((?P<SIZE>.+)\)", row["Type"] )
            if r != None :
                obj[ row["Field"] ][ "type" ] = r.group("TYPE")
                obj[ row["Field"] ][ "size" ] = r.group("SIZE")
            else:
                obj[ row["Field"] ][ "type" ] = row["Type"]
            if row["Default"] != None :
                obj[ row["Field"] ]["default" ] = row["Default"] 
            if row["Extra"] == "auto_increment" :
                obj[ row["Field"] ]["auto_increment" ] = True 
        return obj
