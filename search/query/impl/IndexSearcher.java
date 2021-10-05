package hust.cs.javacourse.search.query.impl;

import hust.cs.javacourse.search.index.AbstractPosting;
import hust.cs.javacourse.search.index.AbstractPostingList;
import hust.cs.javacourse.search.index.AbstractTerm;
import hust.cs.javacourse.search.query.AbstractHit;
import hust.cs.javacourse.search.query.AbstractIndexSearcher;
import hust.cs.javacourse.search.query.Sort;

import java.io.File;
import java.util.*;

/**
 * AbstractIndexSearcher的具体实现类
 */
public class IndexSearcher extends AbstractIndexSearcher {
    /**
     * 从指定索引文件打开索引，加载到index对象里. 一定要先打开索引，才能执行search方法
     *
     * @param indexFile ：指定索引文件
     */
    @Override
    public void open(String indexFile) {
        this.index.load(new File(indexFile));
    }

    /**
     * 根据单个检索词进行搜索
     *
     * @param queryTerm ：检索词
     * @param sorter    ：排序器
     * @return ：命中结果数组
     */
    @Override
    public AbstractHit[] search(AbstractTerm queryTerm, Sort sorter) {
        AbstractPostingList postingList = this.index.search(queryTerm);
        if(postingList == null)
            return null;
        AbstractHit[] Hits = new AbstractHit[postingList.size()];
        AbstractHit Hit;
        AbstractPosting Posting;
        for(int i = 0; i < Hits.length; i++){
            Posting = postingList.get(i);
            Map<AbstractTerm, AbstractPosting> termPostingMapping = new TreeMap<>(); //应该在循环体内实例化
            termPostingMapping.put(queryTerm,Posting);
            Hit = new Hit(Posting.getDocId(),this.index.getDocName(Posting.getDocId()),termPostingMapping);
            sorter.score(Hit);
            Hits[i] = Hit;
        }
        sorter.sort(Arrays.asList(Hits));
        return Hits;
    }

    /**
     * 根据二个检索词进行搜索
     *
     * @param queryTerm1 ：第1个检索词
     * @param queryTerm2 ：第2个检索词
     * @param sorter     ：    排序器
     * @param combine    ：   多个检索词的逻辑组合方式
     * @return ：命中结果数组
     */
    @Override
    public AbstractHit[] search(AbstractTerm queryTerm1, AbstractTerm queryTerm2, Sort sorter, LogicalCombination combine) {
        AbstractPostingList postingList1 = this.index.search(queryTerm1);
        AbstractPostingList postingList2 = this.index.search(queryTerm2);
        //由于列表的操作比较简单，所以我们先对列表进行操作，最后再转换成数组
        List<AbstractHit> hits = new ArrayList<>();
        //跟以前的做法类似，先通过两个检索词找到对应的PostingList，并创建hits

        //如果逻辑词选取的AND
        //考虑二者空的情况，一旦有一个PostingList，那么必不可能成立

        if(combine == LogicalCombination.AND){
            if(postingList1 == null || postingList2 == null){
                return null;
            }
            //考虑不为空的情况
            int i = 0, j = 0;
            AbstractPosting posting1,posting2;

            while(i < postingList1.size() && j < postingList2.size()){
                posting1 = postingList1.get(i);
                posting2 = postingList2.get(j);
                //通过遍历来寻找是否存在相同Id的Posting

                //如果找到Id相同的Posting，证明逻辑词AND能够使用
                if(posting1.getDocId() == posting2.getDocId()){
                    int docId = posting1.getDocId();
                    Map<AbstractTerm, AbstractPosting> termPostingMapping = new TreeMap<>();
                    //创建映射表
                    termPostingMapping.put(queryTerm1,posting1);
                    termPostingMapping.put(queryTerm2,posting2);
                    //加入映射
                    AbstractHit hit = new Hit(docId,index.getDocName(docId),termPostingMapping);
                    //创建hit
                    sorter.score(hit);//记录分数
                    hits.add(hit);//将该hit将入hits数组
                    //操作结束，进入下一个
                    i++;
                    j++;
                }

                //如果Posting1的id小，那么选取他的下一个进行比较
                else if(posting1.getDocId() < posting2.getDocId()){
                    i++;
                }

                //反之，则选取Posting2的下一个Id
                else{
                    j++;
                }
            }

            //寻找完毕
            if(hits.isEmpty())
                return null;
            sorter.sort(hits);//排序

            AbstractHit[] Hits = new AbstractHit[hits.size()];
            hits.toArray(Hits);
            return Hits;
        }


        //如果逻辑词为or
        if(combine == LogicalCombination.OR){

            if(postingList1 == null){
                return search(queryTerm2,sorter);
            }
            else if(postingList2 == null){
                return search(queryTerm1,sorter);
            }
            //考虑没有空的情况

            else{
                int i = 0, j = 0;
                int docId;
                AbstractPosting posting1,posting2;

                while(i < postingList1.size() && j < postingList2.size()) {
                    posting1 = postingList1.get(i);
                    posting2 = postingList2.get(j);

                    //对于两个关键词都同时命中的情况单独处理
                    //如果二者Id相同
                    if (posting1.getDocId() == posting2.getDocId()) {
                        docId = posting1.getDocId();
                        Map<AbstractTerm, AbstractPosting> termPostingMapping = new TreeMap<>();
                        termPostingMapping.put(queryTerm1, posting1);
                        termPostingMapping.put(queryTerm2, posting2);
                        AbstractHit hit = new Hit(docId, index.getDocName(docId), termPostingMapping);
                        sorter.score(hit);
                        hits.add(hit);
                        i++;
                        j++;
                    }

                    //二者Id不相同的情况
                    //为了避免重复，我采取的措施，小的先加入Hit数组

                    else if(posting1.getDocId() < posting2.getDocId()){
                        docId = posting1.getDocId();
                        Map<AbstractTerm, AbstractPosting> termPostingMapping = new TreeMap<>();
                        termPostingMapping.put(queryTerm1, posting1);
                        AbstractHit hit = new Hit(docId, index.getDocName(docId), termPostingMapping);
                        sorter.score(hit);
                        hits.add(hit);
                        i++;
                    }

                    else{
                        docId = posting2.getDocId();
                        Map<AbstractTerm, AbstractPosting> termPostingMapping = new TreeMap<>();
                        termPostingMapping.put(queryTerm2, posting2);
                        AbstractHit hit = new Hit(docId, index.getDocName(docId), termPostingMapping);
                        sorter.score(hit);
                        hits.add(hit);
                        j++;
                    }
                }

                //结束二者共同遍历，这时判断二者哪一个还未遍历完毕，接着遍历
                while( i < postingList1.size()){
                    posting1 = postingList1.get(i);
                    docId = posting1.getDocId();
                    Map<AbstractTerm, AbstractPosting> termPostingMapping = new TreeMap<>();
                    termPostingMapping.put(queryTerm1, posting1);
                    AbstractHit hit = new Hit(docId, index.getDocName(docId), termPostingMapping);
                    sorter.score(hit);
                    hits.add(hit);
                    i++;
                }
                while( j < postingList2.size()){
                    posting2 = postingList2.get(j);
                    docId = posting2.getDocId();
                    Map<AbstractTerm, AbstractPosting> termPostingMapping = new TreeMap<>();
                    termPostingMapping.put(queryTerm2, posting2);
                    AbstractHit hit = new Hit(docId, index.getDocName(docId), termPostingMapping);
                    sorter.score(hit);
                    hits.add(hit);
                    j++;
                }

                //寻找完成
                if(hits.isEmpty())
                    return null;
                sorter.sort(hits);
                AbstractHit[] Hits = new AbstractHit[hits.size()];
                hits.toArray(Hits);
                return Hits;
            }
        }


        return null;
    }
}
