namespace Sitecore.Support
{
    using System;
    using Lucene.Net.Index;
    using Sitecore.ContentSearch.Diagnostics;

    public class ConcurrentMergeScheduler : Lucene.Net.Index.ConcurrentMergeScheduler
    {
        protected override void HandleMergeException(Exception exc)
        {
            try
            {
                base.HandleMergeException(exc);
            }
            catch (Exception exception)
            {
                CrawlingLog.Log.Fatal("SUPPORT LUCENE Merge operation has been finished with exception...", exception);
            }
        }
    }
}