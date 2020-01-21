import mysql.connector
import  arff
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Some password",
    database="some database name"
)
mycursor = mydb.cursor()
names = ["call","device","activity","cs"]

indexes = ["10", "14", "17", "24", "36", "38", "42", "50", "51", "65", "69", "72", "78", "80", "85"]

for databaseName in names:
    for personOid in indexes:
        for c in range(1, 11):
            mycursor.execute("select * from " + databaseName + personOid + "{0:0=2d}".format(c)+"y")
            res = mycursor.fetchall()
            for j in indexes:
                if (j != personOid):
                    mycursor.execute("select * from " + databaseName + j + "{0:0=2d}".format(c)+"n")
                    res2 = mycursor.fetchall()
                    res += res2
            if databaseName == "call":
                obj = {
                   'description': u'',
                   'relation': databaseName+personOid+"{0:0=2d}".format(c),
                   'attributes': [
                       ('phonenumber_oid', 'numeric'),
                       ('cnid', ['1','2']),
                       ('dirid', ['1','2','3']),
                       ('deid', ['1','2','3']),
                       ('dd',
                        ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17',
                         '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']),
                       ('wd', ['1', '2', '3', '4', '5', '6', '7']),
                       ('mn', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']),
                       ('duration', 'numeric'),
                       ('stime', 'numeric'),
                        ('person', ['yes', 'no'])
                   ],
                   'data':res,
                }
            elif databaseName == "device":
                obj = {
                    'description': u'',
                    'relation': databaseName + personOid + "{0:0=2d}".format(c),
                    'attributes': [
                        ('device_oid', 'numeric'),
                        ('dd',
                         ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17',
                          '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']),
                        ('wd', ['1', '2', '3', '4', '5', '6', '7']),
                        ('mn', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']),
                        ('duration', 'numeric'),
                        ('stime', 'numeric'),
                        ('person', ['yes', 'no'])
                    ],
                    'data': res,
                }
            elif databaseName == "activity":
                obj = {
                    'description': u'',
                    'relation': databaseName + personOid + "{0:0=2d}".format(c),
                    'attributes': [
                        ('dd',
                         ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17',
                          '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']),
                        ('wd', ['1', '2', '3', '4', '5', '6', '7']),
                        ('mn', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']),
                        ('duration', 'numeric'),
                        ('stime', 'numeric'),
                        ('person', ['yes', 'no'])
                    ],
                    'data': res,
                }
            elif databaseName == "cs":
                obj = {
                    'description': u'',
                    'relation': databaseName + personOid + "{0:0=2d}".format(c),
                    'attributes': [
                        ('celltower_oid', 'numeric'),
                        ('dd',
                         ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17',
                          '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']),
                        ('wd', ['1', '2', '3', '4', '5', '6', '7']),
                        ('mn', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']),
                        ('duration', 'numeric'),
                        ('stime', 'numeric'),
                        ('person', ['yes', 'no'])
                    ],
                    'data': res,
                }
            arff.dump(obj, open("ARFF_FILES/"+databaseName+personOid+"{0:0=2d}".format(c)+".arff","w+"))

mydb.commit()
mydb.close()
