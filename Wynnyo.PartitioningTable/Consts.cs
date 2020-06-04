using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Wynnyo.PartitioningTable
{
    public static class Consts
    {
        public static string DbName = "MyData"; // 数据库的 name
        public static string TableName = "Log";  // 表的 name
        public static string FilePath = "C:\\MyData"; // 分区表存储的文件夹
        public static string FileName = "Log_Data_"; // 分区表存储的文件夹
        public static int FileSize = 5; // 文件初始大小(MB)
        public static int FileMaxSize = 100; // 文件初始大小(MB)

        public static string PartitionFunctionName = "F_Date_Day"; // 分区函数名称
        public static string PartitionSchemeName = "P_Date_Day"; // 分区方案名称

        public static int TaskDay = 1; // 自动增加分区的时间为 1 天
        public static int ReserveDay = 31; // 预留数据为31天, 这样可以显示完整的一个月数据

        public static int ReservePartitions = 2; // 预分区数量
    }
}
