using System;
using DbUp.Engine;

namespace DbUp.Helpers
{
    /// <summary>
    /// Enables multiple executions of idempotent scripts.
    /// </summary>
    public class NullJournal : IJournal
    {
        /// <summary>
        /// Returns an empty array of length 0
        /// </summary>
        /// <returns></returns>
        public string[] GetExecutedScripts()
        {
            return new string[0];
        }

        public string[] GetExecutedScriptsOnBatchNumber(int batchNumber)
        {
            throw new NotImplementedException();
        }

        public int GetCurrentBatchNumber()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Does not store the script, simply returns
        /// </summary>
        /// <param name="script"></param>
        public void StoreExecutedScript(SqlScript script)
        { }

        public void UpdateScriptEntry(string scriptName)
        {
            throw new NotImplementedException();
        }
    }
}
