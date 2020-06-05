using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Wynnyo.PartitioningTable.Dtos
{
    /// <summary>
    /// 统计分区信息
    /// </summary>
    public class PartitionDto
    {
        /// <summary>
        /// 分区
        /// </summary>
        public string Partition { get; set; }

        /// <summary>
        /// 分区中数据数量
        /// </summary>
        public string Rows { get; set; }

        /// <summary>
        /// 分区中时间最小值
        /// </summary>
        public string MinVal { get; set; }

        /// <summary>
        /// 分区中时间最大值
        /// </summary>
        public string MaxVal { get; set; }
    }
}
