using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileUpload_Second
{
    class Program
    {
        string UserName = ConfigurationManager.AppSettings["FTPUserName"].ToString();
        string Password = ConfigurationManager.AppSettings["FTPPassword"].ToString();
        System.Uri Uri = new Uri("ftp://" + ConfigurationManager.AppSettings["FTPServer"] + ":" + ConfigurationManager.AppSettings["FTPPortNO"]);

        IDatabase db = SeRedis.redis.GetDatabase();
        string WebFilePath = ConfigurationManager.AppSettings["WebFilePath"].ToString();//web路径

        static void Main(string[] args)
        {
            if (ConfigurationManager.AppSettings["AutoRun"].ToString().Trim() == "Y")
            {
                Program p = new Program();
                p.fileupload();
            }
        }

        private void fileupload()
        {
            string sql = string.Empty;
            try
            {
                FtpHelper ftp = new FtpHelper(Uri, UserName, Password);
                DataTable dt;

                //企业端：entid不为空 -》 两天内上传的 且 未上传的
                //报关行：entid为空 -》 两天内上传的 且 未上传的 且 未作废 且 文件类型是44、57、58

                /*string sql = @"select t.* from List_Attachment t 
                                where (t.entid is null or t.entid='') and t.isupload is null and (t.filetype=44 or t.filetype=57 or t.filetype=58)  
                                    and (t.abolishstatus=0 or t.abolishstatus is null) and t.uploadtime>sysdate-interval '2' day                                  
                                order by t.uploadtime desc";*/

                sql = @"
                        select t.* from List_Attachment t
                        where t.isupload is null and t.uploadtime>sysdate-interval '2' day 
                                and (
          	                        (t.entid is not null or t.entid<>'') 
                                    or 
                                    (
            	                        (t.entid is null or t.entid='') and (t.filetype=44 or t.filetype=57 or t.filetype=58) and (t.abolishstatus=0 or t.abolishstatus is null)
                                    )
                                    )
                        order by t.uploadtime desc
                    ";

                dt = DBMgr.GetDataTable(sql);
                string[] array = null;
                foreach (DataRow dr in dt.Rows)
                {
                    array = (dr["FILENAME"] + "").Split(new string[] { "/" }, StringSplitOptions.RemoveEmptyEntries);
                    string filename = array[array.Length - 1];//文件名称     
                    string AppPath = WebFilePath + filename;//web路径+文件名称 

                    string FIlEDIR = "";//ftp路径
                    for (int i = 0; i < array.Length - 1; i++)
                    {
                        FIlEDIR = FIlEDIR + "/" + array[i];
                    }
                    string FIlEPAT = FIlEDIR + "/" + filename;//ftp路径+文件名称       

                    try
                    {
                        bool res = ftp.UploadFile(AppPath, FIlEPAT, true);
                        if (res)
                        {
                            if (dr["FILETYPE"] + "" == "44")//如果是订单文件,在上传完成时自动进行确认 此时上传人即确认人
                            {
                                sql = "update List_Attachment set ISUPLOAD='1',CONFIRMSTATUS=1,confirmer=uploaduserid,confirmtime=uploadtime WHERE ID='" + dr["ID"] + "'";
                            }
                            else
                            {
                                sql = "update List_Attachment set ISUPLOAD='1' WHERE ID='" + dr["ID"] + "'";
                            }
                            DBMgr.ExecuteNonQuery(sql);

                            if (dr["ENTID"].ToString() == "")//企业端上传的文件，不压缩 20160929
                            {
                                AddPdfShrinkTask(dr["ORDERCODE"].ToString());//增加压缩任务          
                            }
                        }

                    }
                    catch (Exception ex)
                    {
                       
                    }
                }
                //删除历史文件
                foreach (string str in Directory.GetFiles(WebFilePath))
                {
                    FileInfo fi = new FileInfo(str);
                    if (fi.CreationTime < DateTime.Now.AddDays(-1))//直接删除2天前的文件
                    {
                        fi.Delete();
                    }
                }
                //监控基础数据变化 如果有变化 update redis
                sql = "select * from sysevent order by datetime asc";
                dt = DBMgrBase.GetDataTable(sql);
                string tablename = "";
                foreach (DataRow dr in dt.Rows)
                {
                    tablename = dr["TABLENAME"] + "";
                    switch (tablename)
                    {
                        case "base_packing"://包装种类       
                            db.KeyDelete("common_data:bzzl");
                            break;
                        case "sys_repway"://申报方式
                            db.KeyDelete("common_data_sbfs: kj");
                            db.KeyDelete("common_data_sbfs:kc");
                            db.KeyDelete("common_data_sbfs:hj");
                            db.KeyDelete("common_data_sbfs:hc");
                            db.KeyDelete("common_data_sbfs:lj");
                            db.KeyDelete("common_data_sbfs:lc");
                            db.KeyDelete("common_data_sbfs:ts");
                            db.KeyDelete("common_data_sbfs:gn");
                            break;
                        case "base_customdistrict"://申报关区 进口口岸
                            db.KeyDelete("common_data:sbgq");
                            break;
                        case "sys_declway"://报关方式
                            db.KeyDelete("common_data:bgfs");
                            break;
                        case "base_decltradeway"://贸易方式
                            db.KeyDelete("common_data:myfs");
                            break;
                        case "base_containertype"://箱型
                            db.KeyDelete("common_data:containertype");
                            break;
                        case "base_containersize"://集装箱尺寸
                            db.KeyDelete("common_data:containersize");
                            break;
                        case "sys_declarationcar"://报关车号
                            db.KeyDelete("common_data:truckno");
                            break;
                    }
                    sql = "delete from sysevent where tablename='" + tablename + "'";
                    DBMgrBase.ExecuteNonQuery(sql);

                }
            }
            catch (Exception ex)
            {

            }
        }

        //增加压缩任务
        public static void AddPdfShrinkTask(string ordercode)
        {
            string sql = @"select t.* from list_attachment t WHERE instr(t.ordercode,'" + ordercode + "')>0 and t.filetype=44 and instr(t.filename,'.pdf')>0";
            DataTable dt = DBMgr.GetDataTable(sql);
            if (dt.Rows.Count == 1)//如果只有一个订单文件且是pdf格式的
            {
                //sql = "select * from pdfshrinklog t where t.attachmentid='" + dt.Rows[0]["ID"] + "'";
                //DataTable dt2 = DBMgr.GetDataTable(sql);
                //if (dt2.Rows.Count == 0)//防止撤销后再次提交 所以次判断一下  为提升性能暂且注释,造成的影响就是多一条shrink日志
                //{
                sql = "insert into pdfshrinklog (id,attachmentid) values (pdfshrinklog_id.nextval,'" + dt.Rows[0]["ID"] + "')";
                DBMgr.ExecuteNonQuery(sql);
                //}
            }
        }

    }
}
