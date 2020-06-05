using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Wynnyo.PartitioningTable.Dtos
{
    /// <summary>
    /// 文件和文件组信息
    /// </summary>
    public class GroupFileDto
    {
        /// <summary>
        /// 文件 name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 文件硬盘位置
        /// </summary>
        public string PhysicalName { get; set; }

        /// <summary>
        /// 文件组 name
        /// </summary>
        public string FileGroup { get; set; }
    }
}
