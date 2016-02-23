namespace DbUp.Engine
{
    /// <summary>
    /// This interface is provided to allow different projects to store version information differently.
    /// </summary>
    public interface IJournal
    {
        /// <summary>
        /// Recalls the version number of the database.
        /// </summary>
        /// <returns></returns>
        string[] GetExecutedScripts();

        /// <summary>
        /// Get scripts executed under a batch number
        /// </summary>
        /// <returns></returns>
        string[] GetExecutedScriptsOnBatchNumber(int batchNumber);

        /// <summary>
        /// Gets the current Version No from the database
        /// </summary>
        /// <returns></returns>
        int GetCurrentBatchNumber();

        /// <summary>
        /// Records an upgrade script for a database.
        /// </summary>
        /// <param name="script">The script.</param>
        void StoreExecutedScript(SqlScript script);

        /// <summary>
        /// Updates script entry with rolledback_ prefix
        /// </summary>
        /// <param name="scriptName">The name of the script to rollback</param>
        void UpdateScriptEntry(string scriptName);

        
    }
}