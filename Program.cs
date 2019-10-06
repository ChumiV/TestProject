using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization.Attributes;
using System.Xml.Serialization;

namespace test_task
{
    [Serializable]
    public class ZL_LIST
    {
        public ZGLV ZGLV { get; set; }
        [XmlElement]
        public EVENT [] EVENT { get; set; }

        static ZL_LIST() { }
    }

    [Serializable]
    public class ZGLV
    {
        public string VERSION { get; set; }
        public string DATA { get; set; }
        public string FILENAME { get; set; }
        public int YEAR { get; set; }
        public string CODE_MO { get; set; }

        ZGLV() { }
    }

    [Serializable]
    public class EVENT
    {
        public string DISP { get; set; }
        public int KOL_M { get; set; }
        public int KOL_W { get; set; }
        [XmlElement]
        public PERS[] PERS { get; set; }

        EVENT() { }
    }

    [Serializable]
    public class PERS
    {
        public string N_ZAP { get; set; }
        public string ID_PAC { get; set; }
        public int W { get; set; }
        public string DR { get; set; }
        public string SMO { get; set; }
        public int VPOLIS { get; set; }
        [BsonIgnoreIfNull]
        public string SPOLIS { get; set; }
        public string NPOLIS { get; set; }
        [BsonIgnoreIfDefault]
        public int QUARTER { get; set; }
        [BsonIgnoreIfDefault]
        public int MONTH { get; set; }
        public string LPU1 { get; set; }
        public string DEPTH { get; set; }
        public string SS_DOC { get; set; }
        [BsonIgnoreIfNull]
        public string SS_DOC_D { get; set; }
        [BsonIgnoreIfDefault]
        public int PRVS_D { get; set; }
        [BsonIgnoreIfNull]
        public string DS_D { get; set; }
        [BsonIgnoreIfDefault]
        public int PLACE_D { get; set; }
        [BsonIgnoreIfNull]
        public string ID_TFOMS { get; set; }
        [BsonIgnoreIfNull]
        public string COMMENT { get; set; }
        [BsonIgnoreIfNull]
        [XmlElement]
        public CONTACTS CONTACTS { get; set; }

        PERS() { }
    }

    [Serializable]
    public class CONTACTS
    {
        [BsonIgnoreIfNull]
        [XmlElement]
        public string[] PHONE_F { get; set; }
        [BsonIgnoreIfNull]
        [XmlElement]
        public string[] PHONE_M { get; set; }
        [BsonIgnoreIfNull]
        [XmlElement]
        public string[] EMAIL { get; set; }
        [BsonIgnoreIfNull]
        [XmlElement]
        public string[] ADDRESS { get; set; }

        CONTACTS() { }
    }

    class Program
    {
        static void Main(string[] args)
        {
            string fileName = "C:\\Users\\chumi\\Downloads\\LPQM750025T75_193.XML";
            string connectionString = "mongodb://localhost:27017";
            string bdName = "TestTask";
            string colName = "ZL_LIST";
           
            ZL_LIST obj= DeserializedFile(fileName);

            if (Verification(obj))
            {
                InputIntoDB(connectionString, bdName, colName, obj.ToBsonDocument());
                Console.WriteLine("Запись успешно добавлена в БД.");
            }
            else
            {
                Console.WriteLine("В файле допущены ошибки. Запись в БД не произведена.");
            }            

            Console.ReadLine();
        }

        static ZL_LIST DeserializedFile(string filePath)
        {            
            XmlSerializer formatter = new XmlSerializer(typeof(ZL_LIST));

            using (FileStream fs = new FileStream(filePath, FileMode.OpenOrCreate))
            {
                ZL_LIST myClass = (ZL_LIST)formatter.Deserialize(fs);
                return myClass;
            }
        }

        static bool Verification(ZL_LIST myClass)
        {
            int num = 0;

            foreach(EVENT el in myClass.EVENT)
            {
                if (el.KOL_M + el.KOL_W != el.PERS.Length)
                {
                    Console.WriteLine("Количество в блоке EVENT[{0}] не соответствует количеству записей в блоке PERS", num);
                    return false;
                }

                num++;
            }

            return true;
        }

        static async Task InputIntoDB(string connectionString, string nameDB, string colName, BsonDocument BsonDoc)
        {
            MongoClient client = new MongoClient(connectionString);
            IMongoDatabase database = client.GetDatabase(nameDB);
            var collection = database.GetCollection<BsonDocument>(colName);

            await collection.InsertOneAsync(BsonDoc);
        }
    }
}
