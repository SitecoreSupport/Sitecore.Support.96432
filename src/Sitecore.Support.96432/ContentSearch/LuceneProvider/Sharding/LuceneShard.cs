namespace Sitecore.Support.ContentSearch.LuceneProvider.Sharding
{
    using Lucene.Net.Index;
    using Lucene.Net.Store;
    using Sitecore.ContentSearch.LuceneProvider;
    using Sitecore.ContentSearch.LuceneProvider.Sharding;
    using Sitecore.ContentSearch.Sharding;
    using Sitecore.ContentSearch.Utilities;
    using Sitecore.Diagnostics;
    using Sitecore.Support;
    using System;
    using System.Reflection;

    public class LuceneShard : Sitecore.ContentSearch.LuceneProvider.Sharding.LuceneShard
    {
        public LuceneShard(ILuceneProviderIndex index, int id, string name, HashRange hashRange) : base(index, id, name, hashRange)
        {
        }

        protected override IndexWriter CreateWriter(Directory directory, LuceneIndexMode mode)
        {
            typeof(Sitecore.ContentSearch.LuceneProvider.Sharding.LuceneShard).GetMethod("EnsureInitialized", BindingFlags.NonPublic | BindingFlags.Instance).Invoke(this, null);
            Assert.ArgumentNotNull(directory, "directory");
            Sitecore.Support.ContentSearch.LuceneProvider.Sharding.LuceneShard shard = this;
            lock (shard)
            {
                bool create = mode == LuceneIndexMode.CreateNew;
                create |= !IndexReader.IndexExists(directory);
                IContentSearchConfigurationSettings instance = base.index.Locator.GetInstance<IContentSearchConfigurationSettings>();
                IndexWriter writer = new IndexWriter(directory, ((LuceneIndexConfiguration)base.index.Configuration).Analyzer, create, IndexWriter.MaxFieldLength.UNLIMITED);
                LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy(writer);
                writer.TermIndexInterval = instance.TermIndexInterval();
                writer.MergeFactor = instance.IndexMergeFactor();
                writer.MaxMergeDocs = instance.MaxMergeDocs();
                writer.UseCompoundFile = instance.UseCompoundFile();
                mp.MaxMergeMB = instance.MaxMergeMB();
                mp.MinMergeMB = instance.MinMergeMB();
                mp.CalibrateSizeByDeletes = instance.CalibrateSizeByDeletes();
                writer.SetMergePolicy(mp);
                writer.SetRAMBufferSizeMB((double)instance.RamBufferSize());
                writer.SetMaxBufferedDocs(instance.MaxDocumentBufferSize());
                Sitecore.Support.ConcurrentMergeScheduler mergeScheduler = new Sitecore.Support.ConcurrentMergeScheduler
                {
                    MaxThreadCount = instance.ConcurrentMergeSchedulerThreads()
                };
                writer.SetMergeScheduler(mergeScheduler);
                typeof(Sitecore.ContentSearch.LuceneProvider.Sharding.LuceneShard).GetField("lastWriterCreated", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(this, writer);
                return writer;
            }
        }
    }
}
