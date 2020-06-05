using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Wynnyo.PartitioningTable.Dtos;
using Wynnyo.PartitioningTable.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Wynnyo.PartitioningTable.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DbController : ControllerBase
    {
        private readonly DbService _dbService;

        public DbController(DbService dbService)
        {
            _dbService = dbService;
        }

        [HttpPost]
        [Route("init")]
        public void Init()
        {
            try
            {
                _dbService.Init();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        [HttpPost]
        [Route("initPartitioningTables")]
        public void InitPartitioningTables()
        {
            try
            {
                _dbService.InitPartitioningTables();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        [HttpPost]
        [Route("partitioningTablesTask")]
        public void PartitioningTablesTask(int addDay = 0)
        {
            try
            {
                _dbService.PartitioningTablesTask(addDay);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        [HttpGet]
        [Route("getGroupFileInfo")]
        public ICollection<GroupFileDto> GetGroupFileInfo()
        {
            try
            {
                var dt = _dbService.GetGroupFileInfo();

                return dt.AsEnumerable().Select(e => new GroupFileDto()
                {
                    Name = e["name"]?.ToString(),
                    FileGroup = e["filegroup"]?.ToString(),
                    PhysicalName = e["physical_name"]?.ToString()
                }).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        [HttpGet]
        [Route("getPartitionInfo")]
        public ICollection<PartitionDto> GetPartitionInfo()
        {
            try
            {
                var dt = _dbService.GetPartitionsInfo();

                return dt.AsEnumerable().Select(e => new PartitionDto
                {
                    Partition = e["PARTITION"]?.ToString(),
                    Rows = e["ROWS"]?.ToString(),
                    MinVal = e["MinVal"]?.ToString(),
                    MaxVal = e["MaxVal"]?.ToString(),
                }).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}