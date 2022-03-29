from ast import NodeTransformer
#from sys import last_value
import xml.dom.minidom as minidom
def main():
    # gunakan fungsi parse() untuk me-load xml ke memori
    # dan melakukan parsing
    doc = minidom.parse("DigitalForecast-DKIJakarta.xml")

    # Cetak isi doc dan tag pertamanya
    print (doc.nodeName)
    print (doc.firstChild.tagName)
    

    name = doc.getElementsByTagName("name")[0].firstChild.data
    parameter = doc.getElementsByTagName("parameter")[0].firstChild.data
   # jurusan = doc.getElementsByTagName("jurusan")[0].firstChild.data
    value = doc.getElementsByTagName("value")[0].firstChild.data

    print "name: {}\nparameter: {}\nvalue: \n".format(name, parameter, value)

    print ("Memiliki {} Value:")

    for value in parameter:
        print "-", value

if __name__ == "__main__":
    main()    