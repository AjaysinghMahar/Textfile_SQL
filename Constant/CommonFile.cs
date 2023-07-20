using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Textfile_SQL.Constant
{
    public static class CommonFile
    {
        public static bool IsTargetFile(string fileName)
        {
            List<string> targetFileNames = new List<string>()
        {
                 "FoundationCompany.txt","Exchange.txt","SecurityDetail.txt", "CompanyCrossRef.txt","SecuritySymbol.txt",
                "SecurityGroup.txt","SecuritySubType.txt","SecurityType.txt","TradingItemSymbol.txt","CompanyIndClass.txt",
                "FoundationSecurity.txt","FoundationTradingItem.txt",
        };

            return targetFileNames.Contains(fileName);
        }
    }
}
