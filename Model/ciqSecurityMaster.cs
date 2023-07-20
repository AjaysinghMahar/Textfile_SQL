using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Textfile_SQL.Model
{
    public class ciqSecurityMaster
    {
  
            public int? SecurityId { get; set; }
        [DisplayFormat(DataFormatString = "{yyyy-MM-dd}", ApplyFormatInEditMode = true)]
        public string? MaturityDate { get; set; }
        
        public decimal? AnnualRate { get; set; }

            public ciqSecurityMaster(int? securityId, string? maturityDate, decimal? annualRate)
            {
                SecurityId = securityId;
                MaturityDate = maturityDate;
                AnnualRate = annualRate;
            }
        
    }
}
