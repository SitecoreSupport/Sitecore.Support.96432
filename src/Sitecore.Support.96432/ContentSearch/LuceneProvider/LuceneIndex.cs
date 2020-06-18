namespace Sitecore.Support.ContentSearch.LuceneProvider
{
    using Lucene.Net.Index;
    using Lucene.Net.Store;
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.LuceneProvider;
    using Sitecore.ContentSearch.LuceneProvider.Sharding;
    using Sitecore.ContentSearch.Maintenance;
    using Sitecore.ContentSearch.Sharding;
    using Sitecore.Diagnostics;
    using Sitecore.IO;
    using Sitecore.Search;
    using Sitecore.Support;
    using Sitecore.Support.ContentSearch.LuceneProvider.Sharding;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.InteropServices;

    public class LuceneIndex : Sitecore.ContentSearch.LuceneProvider.LuceneIndex
    {
        protected LuceneIndex(string name) : base(name)
        {
        }

        public LuceneIndex(string name, string folder, IIndexPropertyStore propertyStore) : base(name, folder, propertyStore)
        {
        }

        public LuceneIndex(string name, string folder, IIndexPropertyStore propertyStore, string group) : base(name, folder, propertyStore, group)
        {
        }

        public override Lucene.Net.Store.Directory CreateDirectory(string folder)
        {
            Assert.ArgumentNotNullOrEmpty(folder, "folder");
            base.EnsureInitialized();
            DirectoryInfo path = new DirectoryInfo(folder);
            FileUtil.EnsureFolder(folder);
            Lucene.Net.Store.FSDirectory directory = Lucene.Net.Store.FSDirectory.Open(path, new Sitecore.ContentSearch.LuceneProvider.SitecoreLockFactory(path.FullName));
            using (new LuceneIndexLocker(directory.MakeLock("write.lock")))
            {
                if (IndexReader.IndexExists(directory))
                {
                    return directory;
                }
                using (IndexWriter writer = new IndexWriter(directory, ((LuceneIndexConfiguration)this.Configuration).Analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED))
                {
                    this.InitializeWithCustomScheduler(writer, -1);
                }
            }
            return directory;
        }

        private Shard CreateNotShardedShard(ISearchIndex index)
        {
            return (Shard)new Sharding.LuceneShard((ILuceneProviderIndex)index, -1, ".[Not Sharded].", new HashRange(int.MinValue, int.MaxValue));
        }        

        protected override void InitializeShards()
        {
            System.Reflection.FieldInfo field = typeof(LuceneIndex).GetField("shards", BindingFlags.NonPublic | BindingFlags.Instance);
            if (!this.IsSharded)
            {
                (field.GetValue(this) as Dictionary<int, Sitecore.ContentSearch.LuceneProvider.Sharding.LuceneShard>)[Shard.NotSharded.Id] = (Sharding.LuceneShard)this.CreateNotShardedShard(this);
            }
            else
            {
                foreach (Sharding.LuceneShard shard in this.ShardingStrategy.GetAllShards().Cast<Sharding.LuceneShard>())
                {
                    (field.GetValue(this) as Dictionary<int, Sitecore.ContentSearch.LuceneProvider.Sharding.LuceneShard>)[shard.Id] = shard;
                }
            }
            foreach (Sitecore.ContentSearch.LuceneProvider.Sharding.LuceneShard shard2 in (field.GetValue(this) as Dictionary<int, Sitecore.ContentSearch.LuceneProvider.Sharding.LuceneShard>).Values)
            {
                shard2.Initialize();
            }
        }


        protected void InitializeWithCustomScheduler(IndexWriter writer, int threadsLimit = -1)
        {
            Sitecore.Support.ConcurrentMergeScheduler mergeScheduler = new Sitecore.Support.ConcurrentMergeScheduler();
            if (threadsLimit != -1)
            {
                mergeScheduler.MaxThreadCount = threadsLimit;
            }
            writer.SetMergeScheduler(mergeScheduler);
        }
    }
}
