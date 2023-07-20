using System.Data.SqlClient;
using System.IO.Compression;
using System.Text.RegularExpressions;
using Textfile_SQL.Constant;
using Textfile_SQL.Model;

namespace TextFileToSQL
{
    class Program
    {
        static void Main()
        {
            bool runSecurityMaster = false;
            string ErrorFolderPath = Path.Combine("D:\\Ajay\\Project\\1Conversol\\S&P\\Textfile_SQL\\", "Error");
            string connectionString = "Data Source=CS-LC-18\\MSSQLSERVER05;Initial Catalog=SandP_Server;Integrated Security=True";

            string zipFilePath = @"C:\Users\CS-LC-18\Products\";
     //Check for the most recent directoty created 
            DirectoryInfo directoryInfo = new DirectoryInfo(zipFilePath);
            DirectoryInfo[] directories = directoryInfo.GetDirectories();

            // Sort the directories by creation date in descending order
             Array.Sort(directories, (a, b) => b.CreationTime.CompareTo(a.CreationTime));
            if (directories.Length > 0)
            {
                DirectoryInfo mostRecentFolder = directories[0];
                zipFilePath += mostRecentFolder.Name;
            try
            {
                string[] zipFiles = Directory.GetFiles(zipFilePath, "*.zip", SearchOption.AllDirectories);

            foreach (string Files in zipFiles)
            {

                string lastValue = Files.Substring(Files.LastIndexOf("\\") + 1);
                if (lastValue.Contains("full", StringComparison.OrdinalIgnoreCase))
                {
                    using (var archive = ZipFile.OpenRead(Files))
                    {
                        foreach (var entry in archive.Entries)
                        {
                            if (CommonFile.IsTargetFile(entry.Name))
                            {
                                        if (TotalRecordInsert(entry.Name, connectionString))
                                        {
                                            Console.WriteLine(lastValue + " File Started");
                                            ProcessTextFileFromZip(Files, entry.Name, connectionString);
                                            Console.WriteLine(lastValue + " File End");
                                        }
                            }

                        }
                    }
                }
            }

                SecurityMasterInsert(connectionString);
             
                // Process the extracted text file

                Console.WriteLine("Records inserted successfully.");
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                string errorFolderPath = ErrorFolderPath;
                string todayDate = DateTime.Today.ToString("yyyy-MM-dd");
                string logFileName = $"{todayDate}.log";
                string logFilePath = Path.Combine(errorFolderPath, todayDate);
                string ErrorFilePath = logFilePath + @"\" + logFileName;
                // Create the "Error" directory if it doesn't exist
                if (!Directory.Exists(logFilePath))
                {
                    Directory.CreateDirectory(logFilePath);
                }

                // Create the log file if it doesn't exist
                if (!File.Exists(ErrorFilePath))
                {
                    File.CreateText(logFileName);

                }
                using (StreamWriter writer = File.AppendText(ErrorFilePath))
                {
                    writer.WriteLine($"Error occurred at {DateTime.Now}");
                    writer.WriteLine($"Error Message: {ex.Message}");
                    writer.WriteLine($"Stack Trace: {ex.StackTrace}");
                    writer.WriteLine();
                }
                Console.WriteLine(ex.Message);
            }
            }
            else
            {
                Console.WriteLine("Directory is Empty ");
            }
        }

        static void ProcessTextFileFromZip(string zipFilePath, string textFileName, string connectionString)
        {

            // Create a SqlConnection object
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                string lastValue = zipFilePath.Substring(zipFilePath.LastIndexOf("\\") + 1);

                // Remove the `.zip` extension from the string.
                lastValue = lastValue.Substring(0, lastValue.Length - 4);
                // Open the connection
                connection.Open();

                // Extract the text file from the ZIP file
                using (ZipArchive archive = ZipFile.OpenRead(zipFilePath))
                {
                    ZipArchiveEntry entry = archive.GetEntry(textFileName);
                    #region Condition Check Object creation 
                    string line;
              
                    bool ComplexData = false;
                    if (textFileName == "FoundationCompany.txt" || textFileName == "Exchange.txt")
                    {
                        ComplexData = true;
                    }
                    string sql = null;
                    string OFF = "";
                    string ON = "";

                    #endregion End of  Condition Check Object creation 


                    try
                    {
                        // Read the text file directly from the ZIP without saving it to a file
                        using (StreamReader reader = new StreamReader(entry?.Open()))
                        {
                            if (!ComplexData)
                            {
                                while ((line = reader.ReadLine()) != null)
                                {
                                    // Parse the line and extract the values
                                    string[] values = line.Split('|');
                                    if (TotalRecordInsert(textFileName, connectionString))
                                    {
                                        // in this keep in mind change table name and check total number of column and check where condition Id 
                                        #region Start of Security Detail Table 
                                        if (textFileName == "SecurityDetail.txt")
                                        {
                                            OFF = $"SET IDENTITY_INSERT ciqSecurityDetail OFF";
                                            ON = $"SET IDENTITY_INSERT ciqSecurityDetail ON";
                                            sql = $" IF NOT EXISTS (SELECT 1 FROM ciqSecurityDetail WHERE securityId = " + values[0] + ") BEGIN INSERT INTO ciqSecurityDetail(securityId,primaryListingExchangeId,deleteReason,isoCfiCode,issueDate,maturityDate,parValue,couponRate,currencyId,CIC,usCFICode,PackageName)" +
                                                   $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])}, {RecognizeValueType(values[2])}, {RecognizeValueType(values[3])}, {RecognizeValueType(values[4])}, {RecognizeValueType(values[5])}, {RecognizeValueType(values[6])}, {RecognizeValueType(values[7])}, {RecognizeValueType(values[8])}, {RecognizeValueType(values[9])}, {RecognizeValueType(values[10])},{RecognizeValueType(lastValue)});End";
                                        }
                                        #endregion End of Security Detail Table 
                                        #region Start of Company Cross Ref Table
                                        if (textFileName == "CompanyCrossRef.txt")
                                        {
                                            OFF = $"SET IDENTITY_INSERT ciqCompanyCrossRef OFF";
                                            ON = $"SET IDENTITY_INSERT ciqCompanyCrossRef ON";
                                            sql = $" IF NOT EXISTS (SELECT 1 FROM ciqCompanyCrossRef WHERE identifierId = " + values[0] + ") BEGIN INSERT INTO ciqCompanyCrossRef(identifierId ,\r\n\tcompanyId ,\r\n\tidentifierValue ,\r\n\tidentifierTypeId,\r\n\tstartDate,\r\n\tendDate ,\r\n\tactiveFlag ,\r\n\tprimaryFlag,PackageName)" +
                                                    $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])}, {RecognizeValueType(values[2])}, {RecognizeValueType(values[3])}, {RecognizeValueType(values[4])}, {RecognizeValueType(values[5])}, {RecognizeValueType(values[6])}, {RecognizeValueType(values[7])},{RecognizeValueType(lastValue)});End";
                                        }
                                        #endregion End of Company Cross Ref Table
                                        #region Start of Security Symbol Table
                                        if (textFileName == "SecuritySymbol.txt")
                                        {
                                            OFF = $"SET IDENTITY_INSERT ciqSecurityIdentifier OFF";
                                            ON = $"SET IDENTITY_INSERT ciqSecurityIdentifier ON";
                                            sql = $"IF NOT EXISTS (SELECT 1 FROM ciqSecurityIdentifier WHERE securityId = " + values[0] + ") BEGIN INSERT INTO ciqSecurityIdentifier(securityId,\tidentifierTypeId,identifierValue,identifierStartDate,identifierEndDate,activeFlag,PackageName)" +
                                                    $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])}, {RecognizeValueType(values[2])}, {RecognizeValueType(values[3])}, {RecognizeValueType(values[4])}, {RecognizeValueType(values[5])},{RecognizeValueType(lastValue)});End";
                                        }
                                        #endregion End of Security Symbol Table

                                        #region Start of Security Group Table 
                                        if (textFileName == "SecurityGroup.txt")
                                        {
                                            OFF = $"SET IDENTITY_INSERT ciqSecurityGroup OFF";
                                            ON = $"SET IDENTITY_INSERT ciqSecurityGroup ON";
                                            sql = $"IF NOT EXISTS (SELECT 1 FROM ciqSecurityGroup WHERE securityGroupId = " + values[0] + ") BEGIN INSERT INTO ciqSecurityGroup(securityGroupId,securityGroupName,PackageName)" +
                                                    $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])},{Convert.ToString(RecognizeValueType(lastValue))});End";
                                        }
                                        #endregion End of Security  Group Table
                                        #region Start of Security SubType Table 
                                        if (textFileName == "SecuritySubType.txt")
                                        {
                                            OFF = $"SET IDENTITY_INSERT ciqSecuritySubType OFF";
                                            ON = $"SET IDENTITY_INSERT ciqSecuritySubType ON";
                                            sql = $" IF NOT EXISTS (SELECT 1 FROM ciqSecuritySubType WHERE securitySubTypeId = " + values[0] + ") BEGIN INSERT INTO ciqSecuritySubType(securitySubTypeId,securitySubTypeName,securityTypeId,PackageName)" +
                                                    $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])}, {RecognizeValueType(values[2])},{RecognizeValueType(lastValue)});End";
                                        }
                                        #endregion End of Security SubType Table 
                                        #region Start of SecurityType Table 
                                        if (textFileName == "SecurityType.txt")
                                        {
                                            OFF = $"SET IDENTITY_INSERT ciqSecurityType OFF";
                                            ON = $"SET IDENTITY_INSERT ciqSecurityType ON";
                                            sql = $"IF NOT EXISTS (SELECT 1 FROM ciqSecurityType WHERE securityTypeId = " + values[0] + ") BEGIN INSERT INTO ciqSecurityType(securityTypeId,securityTypeName,securityGroupId,PackageName)" +
                                                    $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])},{RecognizeValueType(values[2])},{RecognizeValueType(lastValue)});End";
                                        }
                                        #endregion End of SecurityType Table

                                        #region Start of Trading Item Identifiers Table 
                                        if (textFileName == "TradingItemSymbol.txt")
                                        {
                                            OFF = $"SET IDENTITY_INSERT ciqTradingItemIdentifier OFF";
                                            ON = $"SET IDENTITY_INSERT ciqTradingItemIdentifier ON";
                                            sql = $"IF NOT EXISTS (SELECT 1 FROM ciqTradingItemIdentifier WHERE tradingItemId = " + values[0] + ") BEGIN INSERT INTO ciqTradingItemIdentifier(tradingItemId,identifierTypeId,identifierValue,identifierStartDate,identifierEndDate,activeFlag,PackageName)" +
                                                   $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])}, {RecognizeValueType(values[2])}, {RecognizeValueType(values[3])}, {RecognizeValueType(values[4])}, {RecognizeValueType(values[5])},{RecognizeValueType(lastValue)});End";
                                        }
                                        #endregion End of Trading Item Identifiers  Table
                                        #region Start of Company Industry Class Table 
                                        if (textFileName == "CompanyIndClass.txt")
                                        {
                                            OFF = $"SET IDENTITY_INSERT ciqCompanyIndClass OFF";
                                            ON = $"SET IDENTITY_INSERT ciqCompanyIndClass ON";
                                            sql = $"IF NOT EXISTS (SELECT 1 FROM ciqCompanyIndClass WHERE CompanyId = " + values[0] + ") BEGIN INSERT INTO ciqCompanyIndClass(CompanyId,industryClassTypeId,industryClassCodeId,primaryFlag,PackageName)" +
                                                   $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])}, {RecognizeValueType(values[2])}, {RecognizeValueType(values[3])},{RecognizeValueType(lastValue)});End";
                                        }
                                        #endregion End of Company Industry Class Table
                                        
                                        #region Start of Company Foundation Security Table 
                                        if (textFileName == "FoundationSecurity.txt")
                                        {
                                      
                                            OFF = $"SET IDENTITY_INSERT ciqFoundationSecurity OFF";
                                            ON = $"SET IDENTITY_INSERT ciqFoundationSecurity ON";
                                            sql = $"IF NOT EXISTS (SELECT 1 FROM ciqFoundationSecurity WHERE securityId = " + values[0] + ") BEGIN INSERT INTO ciqFoundationSecurity(securityId,issuerId,companyId,securityName,securitySubTypeId,activeFlag,primaryFlag,lastUpdateDate,CUSIP,PackageName)" +
                                                   $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])}, {RecognizeValueType(values[2])}, {RecognizeValueType(values[3])}, {RecognizeValueType(values[4])}, {RecognizeValueType(values[5])}, {RecognizeValueType(values[6])}, {RecognizeValueType(values[7])}, {RecognizeValueType(values[8])},{RecognizeValueType(lastValue)});End";
                                        }
                                        #endregion End of Company Industry Class Table
                                        #region Start of Foundation TradingI tem Table 
                                        if (textFileName == "FoundationTradingItem.txt")
                                        {
                                            OFF = $"SET IDENTITY_INSERT ciqFoundationTradingItem OFF";
                                            ON = $"SET IDENTITY_INSERT ciqFoundationTradingItem ON";
                                            sql = $"IF NOT EXISTS (SELECT 1 FROM ciqFoundationTradingItem WHERE tradingItemId = " + values[0] + ") BEGIN INSERT INTO ciqFoundationTradingItem(tradingItemId,securityId,currencyId,exchangeId,activeFlag,lastUpdateDate,PackageName)" +
                                                   $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])}, {RecognizeValueType(values[2])}, {RecognizeValueType(values[3])},{RecognizeValueType(values[4])},{RecognizeValueType(values[5])},{RecognizeValueType(lastValue)});End";
                                        }
                                        #endregion End of Company Industry Class Table
                                        using (SqlCommand disableIdentityInsertCmd = new SqlCommand(ON, connection))
                                        {
                                            disableIdentityInsertCmd.ExecuteNonQuery();
                                        }

                                        // Create a SqlCommand object
                                        using (SqlCommand command = new SqlCommand(sql, connection))
                                        {
                                            // Execute the INSERT statement
                                            command.ExecuteNonQuery();
                                        }

                                        using (SqlCommand disableIdentityInsertCmd = new SqlCommand(OFF, connection))
                                        {
                                            disableIdentityInsertCmd.ExecuteNonQuery();
                                        }
                                    }
                                }
                            }

                            #region Start of Company Table 
                            if (textFileName == "FoundationCompany.txt")
                            {
                                char[] buffer = new char[4096]; // Set the buffer size to your desired value
                                int bytesRead;
                                string remainingData = string.Empty;

                                while ((bytesRead = reader.Read(buffer, 0, buffer.Length)) > 0)
                                {
                                    string chunk = new string(buffer, 0, bytesRead);
                                    string[] rows;

                                    if (chunk.EndsWith("#@#@#"))
                                    {
                                        rows = (remainingData + chunk).Split(new string[] { "#@#@#" }, StringSplitOptions.RemoveEmptyEntries);
                                        remainingData = string.Empty;
                                    }
                                    else
                                    {
                                        rows = (remainingData + chunk).Split(new string[] { "#@#@#" }, StringSplitOptions.RemoveEmptyEntries);
                                        remainingData = rows[rows.Length - 1];
                                        rows = rows.Take(rows.Length - 1).ToArray();
                                    }

                                    // Process each row of data here
                                    foreach (string row in rows)
                                    {
                                        OFF = $"SET IDENTITY_INSERT ciqCompany OFF";
                                        ON = $"SET IDENTITY_INSERT ciqCompany ON";

                                        string[] values = row.Split(new string[] { "'~'" }, StringSplitOptions.None);
                                     
                                        sql = $"IF NOT EXISTS (SELECT 1 FROM ciqCompany WHERE CompanyId = " + values[0] + ") BEGIN INSERT INTO ciqCompany(CompanyId,companyName,city,companyStatusTypeId,companyTypeId,officeFaxValue,officePhoneValue,otherPhoneValue,\r\n\tsimpleIndustryId,streetAddress,streetAddress2,streetAddress3,streetAddress4,yearFounded,monthFounded,dayFounded,\r\n\tzipCode,webpage,reportingTemplateTypeId,countryId,stateId,incorporationCountryId,incorporationStateId,PackageName)" +
                                            $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])}, {RecognizeValueType(values[2])}, {RecognizeValueType(values[3])}, {RecognizeValueType(values[4])}, {RecognizeValueType(values[5])}, {RecognizeValueType(values[6])}, {RecognizeValueType(values[7])}, {RecognizeValueType(values[8])}, {RecognizeValueType(values[9])}, {RecognizeValueType(values[10])},{RecognizeValueType(values[11])},{RecognizeValueType(values[12])},{RecognizeValueType(values[13])},{RecognizeValueType(values[14])},{RecognizeValueType(values[15])},{RecognizeValueType(values[16])},{RecognizeValueType(values[17])},{RecognizeValueType(values[18])},{RecognizeValueType(values[19])},{RecognizeValueType(values[20])},{RecognizeValueType(values[21])},{RecognizeValueType(values[22])},{RecognizeValueType(lastValue)});End";

                                        using (SqlCommand disableIdentityInsertCmd = new SqlCommand(ON, connection))
                                        {
                                            disableIdentityInsertCmd.ExecuteNonQuery();
                                        }

                                        // Create a SqlCommand object
                                        using (SqlCommand command = new SqlCommand(sql, connection))
                                        {
                                            // Execute the INSERT statement
                                            command.ExecuteNonQuery();
                                        }

                                        using (SqlCommand disableIdentityInsertCmd = new SqlCommand(OFF, connection))
                                        {
                                            disableIdentityInsertCmd.ExecuteNonQuery();
                                        }




                                        Array.Clear(buffer, 0, bytesRead);
                                    }
                                }
                            }
                            if (textFileName == "Exchange.txt")
                            {
                                char[] buffer = new char[4096]; 
                                int bytesRead;
                                string remainingData = string.Empty;

                                while ((bytesRead = reader.Read(buffer, 0, buffer.Length)) > 0)
                                {
                                    string chunk = new string(buffer, 0, bytesRead);
                                    string[] rows;

                                    if (chunk.EndsWith("#@#@#"))
                                    {
                                        rows = (remainingData + chunk).Split(new string[] { "#@#@#" }, StringSplitOptions.RemoveEmptyEntries);
                                        remainingData = string.Empty;
                                    }
                                    else
                                    {
                                        rows = (remainingData + chunk).Split(new string[] { "#@#@#" }, StringSplitOptions.RemoveEmptyEntries);
                                        remainingData = rows[rows.Length - 1];
                                        rows = rows.Take(rows.Length - 1).ToArray();
                                    }

                                    // Process each row of data here
                                    foreach (string row in rows)
                                    {
                                        OFF = $"SET IDENTITY_INSERT ciqExchange OFF";
                                        ON = $"SET IDENTITY_INSERT ciqExchange ON";

                                        string[] values = row.Split(new string[] { "'~'" }, StringSplitOptions.None);

                                        sql = $"IF NOT EXISTS (SELECT 1 FROM ciqExchange WHERE Exchangeld = " + values[0] + ") BEGIN INSERT INTO ciqExchange(Exchangeld,ExchangeName,ExchangeSymbol,ImportanceLevel,CurrencyId,Countryld,PackageName)" +
                                            $"VALUES ({RecognizeValueType(values[0])}, {RecognizeValueType(values[1])}, {RecognizeValueType(values[2])}, {RecognizeValueType(values[3])}, {RecognizeValueType(values[4])}, {RecognizeValueType(values[5])}, {RecognizeValueType(lastValue)});End";

                                        using (SqlCommand disableIdentityInsertCmd = new SqlCommand(ON, connection))
                                        {
                                            disableIdentityInsertCmd.ExecuteNonQuery();
                                        }

                                        // Create a SqlCommand object
                                        using (SqlCommand command = new SqlCommand(sql, connection))
                                        {
                                            // Execute the INSERT statement
                                            command.ExecuteNonQuery();
                                        }

                                        using (SqlCommand disableIdentityInsertCmd = new SqlCommand(OFF, connection))
                                        {
                                            disableIdentityInsertCmd.ExecuteNonQuery();
                                        }




                                        Array.Clear(buffer, 0, bytesRead);
                                    }
                                }
                            }

                            #endregion  End of Company Table 
                        }
                    }
                    catch (NullReferenceException ex)
                    {
                        // Handle the null reference error.
                        Console.WriteLine(ex.Message);
                    }
                }
                // Close the connection
                connection.Close();



            }
        }

        static void SecurityMasterInsert(string connectionString)
        {
            string OFF = $"SET IDENTITY_INSERT ciqSecurityMaster OFF";
            string ON = $"SET IDENTITY_INSERT ciqSecurityMaster ON";
            string sqlQuery = @"SELECT CSD.securityId, CSD.maturityDate, CSD.parValue as AnnualRate
                    FROM ciqSecurityDetail CSD
                    INNER JOIN CiqCurrency CC ON CC.CurrencyId = CSD.CurrencyId
                    INNER JOIN Country C ON C.CountryId = CC.CountryId";

            Console.WriteLine("Security Master  Started.");
            List<object> records = new List<object>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                // Open the connection
                connection.Open();

                // Create the command
                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {
                    // Execute the reader
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        // Read the records and store them in the list
                        while (reader.Read())
                        {
                            int? securityId = reader["securityId"] != DBNull.Value ? Convert.ToInt32(reader["securityId"]) : (int?)null;
                            string? maturityDate = reader["maturityDate"] != DBNull.Value ? Convert.ToString(reader["maturityDate"]) : (string?)null;
                            decimal? annualRate = reader["AnnualRate"] != DBNull.Value ? Convert.ToDecimal(reader["AnnualRate"]) : (decimal?)null;

                            ciqSecurityMaster record = new ciqSecurityMaster(securityId, maturityDate, annualRate);
                            records.Add(record);
                        }
                    }
                }
            }

            // Insert the records into ciqSecurityMaster
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                // Open the connection
                connection.Open();

                // Insert the records
                foreach (ciqSecurityMaster record in records)
                {
                    if (record.MaturityDate != "" && record.MaturityDate != null)
                    {
                        DateTime? dateTime = Convert.ToDateTime(record?.MaturityDate); // or any DateTime value

                        string formattedDateTime = dateTime?.ToString("yyyy/MM/dd");
                        record.MaturityDate = formattedDateTime;
                    }

                    string insertQuery = $"IF NOT EXISTS (SELECT 1 FROM ciqSecurityMaster WHERE SecurityId = " + record.SecurityId + ") BEGIN INSERT INTO ciqSecurityMaster(SecurityId, MaturityDate, AnnualRate)" +
                                              $"VALUES ({RecognizeValueType(record.SecurityId)}, {RecognizeValueType(record.MaturityDate)}, {RecognizeValueType(record.AnnualRate)});End";

                    using (SqlCommand disableIdentityInsertCmd = new SqlCommand(ON, connection))
                    {
                        disableIdentityInsertCmd.ExecuteNonQuery();
                    }
                    using (SqlCommand command = new SqlCommand(insertQuery, connection))
                    {

                        // Execute the insertion
                        command.ExecuteNonQuery();
                    }
                    using (SqlCommand disableIdentityInsertCmd = new SqlCommand(OFF, connection))
                    {
                        disableIdentityInsertCmd.ExecuteNonQuery();
                    }
                }
            }
            Console.WriteLine("Security Master  End.");
        }

        static object RecognizeValueType(object value)
        {

            Type valueType = value?.GetType();


            if (value == null || value.ToString().Trim() == "")
            {
                return "null";
            }

            // Check if the value is an integer.
            Regex regexInt = new Regex(@"^[0-9]+$");
            if (regexInt.IsMatch(value.ToString().Trim()))
            {
                return value;
            }
           
            string stringValue = value.ToString();
            int count = stringValue.Count(c => c == '.');
            bool containsHyphens = value.ToString().Contains(@"-*");
            if (Regex.IsMatch(value.ToString().Trim(), @"[a-zA-Z\s]") || Regex.IsMatch(value.ToString().Trim(), @"[~`!@#$%^&*()_+-=]") || containsHyphens || count > 1)
            {
                string escapedValue = value.ToString().Replace("'", "''");
                return "'" + escapedValue + "'";
            }
            Regex regexDate = new Regex(@"\b(?:\d{1,4}[-/]\d{1,2}[-/]\d{1,4}|\d{1,2}[-/]\d{1,2}[-/]\d{1,4}|\d{1,4}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2})\b");
            if (regexDate.IsMatch(value.ToString().Trim()))
            {
                DateTime? dateTime = Convert.ToDateTime(value); // or any DateTime value

                string formattedDateTime = dateTime?.ToString("yyyy/MM/dd HH:mm:ss");
                value = formattedDateTime;

            }
            Regex regexNumeric = new Regex(@"^[0 - 9] + (\.\d +)?$");
            if (regexNumeric.IsMatch(value.ToString().Trim()))
            {
                valueType = typeof(decimal);
            }

            // Return the value and the type.
            object convertedValue = Convert.ChangeType(value, valueType);
            return convertedValue;
        }

        static bool TotalRecordInsert(string textFileName,string connectionString)
        {
            var sql = "";
       
            if (textFileName != "" && textFileName != null)
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    if (textFileName == "FoundationCompany.txt")
                    {
                        sql = "SELECT COUNT(*) FROM ciqCompany";
                    }
                    else if (textFileName == "Exchange.txt")
                    {
                        sql = "SELECT COUNT(*) FROM ciqExchange";
                    }
                    else if (textFileName == "SecurityDetail.txt") { sql = "SELECT COUNT(*) FROM ciqSecurityDetail"; }
                    else if (textFileName == "CompanyCrossRef.txt") { sql = "SELECT COUNT(*) FROM ciqCompanyCrossRef"; }
                    else if (textFileName == "SecuritySymbol.txt") { sql = "SELECT COUNT(*) FROM ciqSecurityIdentifier"; }
                    else if (textFileName == "SecurityGroup.txt") { sql = "SELECT COUNT(*) FROM ciqSecurityGroup"; }
                    else if (textFileName == "SecuritySubType.txt") { sql = "SELECT COUNT(*) FROM ciqSecuritySubType"; }
                    else if (textFileName == "SecurityType.txt") { sql = "SELECT COUNT(*) FROM ciqSecurityType"; }
                    else if (textFileName == "TradingItemSymbol.txt") { sql = "SELECT COUNT(*) FROM ciqTradingItemIdentifier"; }
                    else if (textFileName == "CompanyIndClass.txt") { sql = "SELECT COUNT(*) FROM ciqCompanyIndClass"; }
                    else if (textFileName == "FoundationSecurity.txt") { sql = "SELECT COUNT(*) FROM ciqFoundationSecurity"; }
                    else if (textFileName == "FoundationTradingItem.txt") { sql = "SELECT COUNT(*) FROM ciqFoundationTradingItem"; }

                        using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        int rowCount = (int)command.ExecuteScalar();
                        // Execute the INSERT statement
                      return   rowCount < 200000;
                    }
                    connection.Close();
                }
            }
            return false;
        }
        

    }
}

