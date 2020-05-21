using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;
using ShZiper;

namespace FetchBlobHamcaran
{
    public partial class Form1 : Form
    {
        public List<string> imageserial = null;

        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            
        }

        private void StartFetch()
        {
            DataAccess.ConnectToDB(DataAccess.ConnectionStringType.Other);
            DataTable dtRefid = DataAccess.ExecuteDataTable("select distinct RefId FROM OFC.offScript");
            foreach (DataRow row in dtRefid.Rows)
            {
                label1.Text = row["refid"].ToString();
                Application.DoEvents();
                try
                {
                    var BlobRows =
                            DataAccess.ExecuteDataTable(
                                    "SELECT  ScrptId, ScrptExt,ScrptBlob,ZipType,reftype , DATALENGTH(ScrptBlob) AS length  FROM OFC.offScript where refid = " + row["RefId"]);

                    CreateFileBlob(BlobRows, row["Refid"].ToString(), "E:\\Blob\\");
                }
                catch (Exception ex)
                {
                    continue;
                }
            }
        }

        private void CreateFileBlob(DataTable dataTable, string id, string filePath)
        {
            try{

            var fileNames = SaveBainaryFieldToFile(dataTable,id, filePath, "Attach", "zip", "A", "");

            //foreach (DataRow row in dataTable.Rows)
            //{
            //    foreach (var fName in fileNames)
            //    {
            //        var ScrptExt = row["ScrptExt"].ToString().ToLower();
            //        var fileName = filePath + "$offT" + fName + "I" + id + "." + ScrptExt;
            //        if (!File.Exists(fileName))
            //        {
            //            ScrptExt = row["ScrptExt"].ToString().ToUpper();
            //            fileName = filePath + "$offT" + fName + "I" + id + "." + ScrptExt;
            //            if (!File.Exists(fileName)) continue;
            //        }

            //    }

                GC.Collect();
            //}

            }
                catch(Exception ex)
                {
                    return;
                }
        }

        public string[] SaveBainaryFieldToFile(DataTable dataTable, string refID, string path, string fileName, string extention, string type, string tempfilename)
        {
            try{

            if (dataTable == null || dataTable.Rows.Count == 0)
                return null;

            if (String.IsNullOrEmpty(path)) return null;
            if (String.IsNullOrEmpty(fileName)) return null;
            if (String.IsNullOrEmpty(extention)) return null;

            path = path.IndexOf("\\") > 0 ? path : String.Format("{0}\\", path);
            extention = extention.IndexOf(".") > 0 ? extention : String.Format(".{0}", extention);
            string refid = "2", oldrefid = null;
            string newFileName = null;
            int i = 0;
            string[] srefType = new string[dataTable.Rows.Count];
            imageserial = new List<string>();
            foreach (DataRow row in dataTable.Rows)
            {
                label2.Text = row["ScrptId"].ToString();
                var refType = row["RefType"].ToString();
                var content = row["ScrptBlob"];
                var ziptype = row["ZipType"].ToString();
                if (content == null) continue;
                if (!(content is byte[])) continue;
                if (!Directory.Exists(path)) Directory.CreateDirectory(path);
                var fileContent = (byte[])content;
                newFileName = path + fileName + refid + ((ziptype == "1") ? extention : "." + row["ScrptExt"]);
                if ((ziptype != "1") && (tempfilename != ""))
                    newFileName = tempfilename;
                srefType[i] = refType;
                i++;
                int perfsize = 0;
                if (i == 1)
                {
                    CreateBinaryFile(newFileName, fileContent, FileMode.Create, row["ScrptId"].ToString(), path, ref perfsize);
                }

                else
                {
                    CreateBinaryFile(newFileName, fileContent, FileMode.Append, row["ScrptId"].ToString(), path, ref perfsize);
                }

                imageserial.Add(row["ScrptId"].ToString());
                Application.DoEvents();
            }
            dataTable.Dispose();
            return srefType;
            }
            catch (Exception ex)
            {
                return null;
            }

        }

        public void CreateBinaryFile(string fileName, byte[] fileContent, FileMode fileMode, string fldserial, string path, ref int pervzise)
        {
            try
            {
                if (fileName.ToLower().Contains("zip"))
                {
                    var fileStream = new FileStream(fileName, fileMode);
                    fileStream.Write(fileContent, 0, fileContent.Length);
                    fileStream.Close();
                    fileStream.Dispose();
                    var azip = ShZip.ExtractZip(fileName, Path.GetDirectoryName(fileName));
                    File.Delete(fileName);
                }
                else
                {
                    var fileStream = new FileStream(fileName, fileMode);
                    fileStream.Write(fileContent, 0, fileContent.Length);
                    fileStream.Close();
                }
            }
            catch (Exception ex)
            {
                return;
            }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            StartFetch();
        }
    }
}
