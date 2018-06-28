#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include "test.h"

#define PAGE_SIZE 4096
#define MAX_FREE 1000
#define MAX_SUB_NUM ((PAGE_SIZE-sizeof(NodeHdr))/sizeof(BtreeRecord))
#define RIGHT_NODE 0
#define LEFT_NODE 1

typedef struct Btree Btree;
typedef struct Node Node;

typedef struct BtreeRecord
{
	int key;
	u8 aMagic[4];
	u8 aData[8];
	u32 iSub;
}BtreeRecord;


/*在Btree中把删除后没有再使用的页用链表连接起来
 *使用时直接从表头取出即可一个空闲页即可
 */
typedef struct FreeSlot FreeSlot;
typedef struct FreeSlot
{
	int iPage;
	FreeSlot *pNext;
}FreeSlot;


struct Btree
{
	int fd;
	Node *pNode;
	Node *pRoot;
    //存储查找结果的内容
	BtreeRecord record;
	u32 iPage;
	u32 iParent;
	u32 iNeighbor;
	u32 iRecord;
	u32 mxRecord;
	/*该标志位置位表示pNode已经分配了内存空间
	 * 把查找到的结点放在pNode
	 */
	u8 getFlag;
	int nFree;
	FreeSlot* pFree;
	/*用来记录查找的路径
	 * 在调整时方便确定父结点位置
	 */
	u32 aFind[10];
	u32 iFind;

};

typedef struct NodeHdr
{
	u32 nPage;
	u32 reserve;
	u8  aMagic[4];
	u16 nRecord;
	u8  isLeaf;
	u8  isRoot;
}NodeHdr;

struct Node
{
	NodeHdr nodeHdr;
	BtreeRecord aRecord[MAX_SUB_NUM];
};


int OsRead(
  int fd,
  void *zBuf,
  int iAmt,
  long iOfst
){
  off_t ofst;
  int nRead;

  ofst = lseek(fd, iOfst, SEEK_SET);
  if( ofst!=iOfst ){
    return 0;
  }
  nRead = read(fd, zBuf, iAmt);

  return nRead;
}


int OsWrite(
  int fd,
  void *zBuf,
  int iAmt,
  long iOfst
){
  off_t ofst;
  int nWrite;

  ofst = lseek(fd, iOfst, SEEK_SET);
  if( ofst!=iOfst ){
    return 0;
  }
  nWrite = write(fd, zBuf, iAmt);

  return nWrite;
}

Btree *OpenBtree(char *zName)
{
	NodeHdr *pHdr;
	remove(zName);
	Btree *pBt=(Btree*)malloc(sizeof(Btree));
	memset(pBt,0,sizeof(Btree));
	pBt->pRoot = (Node *)malloc(PAGE_SIZE);
	pBt->fd = open(zName, O_RDWR|O_CREAT, 0600);
	OsRead(pBt->fd,pBt->pRoot,PAGE_SIZE,0);
	pHdr = &pBt->pRoot->nodeHdr;
	//导入根结点，如果没有先初始化
	if( memcmp(pHdr->aMagic,aMagic,4)!=0 )
	{
		memset(pBt->pRoot,0,PAGE_SIZE);
		pHdr->nRecord = 0;
		pHdr->isRoot = 1;
		pHdr->isLeaf = 1;
		pHdr->nPage = 1;
		memcpy(pHdr->aMagic,aMagic,4);
		OsWrite(pBt->fd,pBt->pRoot,PAGE_SIZE,0);
	}

	return pBt;
}

void updateRoot(Btree *pBt, Node *p)
{
    if(p->nodeHdr.isRoot)
    {
    	memcpy(pBt->pRoot, p, PAGE_SIZE);
    }
}

void CloseBtree(Btree *pBt)
{
	close(pBt->fd);
	free(pBt->pRoot);
	free(pBt);
}

void addFreeSlot(Btree *pBt, u32 iPage)
{
    if(pBt->nFree<MAX_FREE && iPage!=0)
    {
		FreeSlot *p = (FreeSlot*)malloc(sizeof(FreeSlot));
		p->iPage = iPage;
		p->pNext = pBt->pFree;
		pBt->pFree = p;
		pBt->nFree++;
    }
}

u32 popFreeSlot(Btree *pBt)
{
	u32 iPage;
	FreeSlot *p = pBt->pFree;
	pBt->pFree = p->pNext;
	pBt->nFree--;
	assert( pBt->nFree>= 0);
	iPage = p->iPage;
	free(p);
	return iPage;
}

/*
 * iPage为当前结点地址
 */
int searchRecord(Btree *pBt,Node *pNode,int key,int iPage)
{
	int i;
	u8 szRecord = sizeof(BtreeRecord);
	NodeHdr *pHdr;
	int rc = 0;

	pHdr = &pNode->nodeHdr;

	//BTree关键字数量的定义
	assert( pHdr->nRecord<MAX_SUB_NUM );
	assert( (pHdr->nRecord>=(MAX_SUB_NUM+1)/2-1) || pHdr->isRoot );

	//查找关键字在结点内偏移，没找到则获取子结点地址
	for( i=0; i<pHdr->nRecord; i++)
	{
		if( pNode->aRecord[i].key>=key )
		{
			if( pNode->aRecord[i].key==key )
			{
				if( pBt->getFlag )
				{
					memcpy( pBt->pNode, pNode, PAGE_SIZE);
				}
				rc = 1;
			}
			break;
		}
	}
	pBt->iRecord = i;
	pBt->iPage = iPage;
	memcpy(&pBt->record, &pNode->aRecord[i], szRecord);

	return rc;
}

int BtreeFind(Btree *pBt, int key)
{
	Node *pNode =(Node *)malloc(PAGE_SIZE);
	int iPage = 0;
	int rc = 0;

	memcpy(pNode, pBt->pRoot, PAGE_SIZE);
	pBt->iFind = 0;
	while( 1 )
	{

		if( searchRecord(pBt,pNode,key,iPage) )
		{
			//追踪查找路径，在后续调整中确定父结点位置
			pBt->aFind[pBt->iFind++] = pBt->iPage;
			rc = 1;
			goto find_finish;
		}
		pBt->aFind[pBt->iFind++] = pBt->iPage;

		if( pNode->nodeHdr.isLeaf )
		{
			break;
		}
		//没找到继续搜索子结点，到达叶子结点后结束
		iPage = pBt->record.iSub;
		assert(iPage);
		OsRead(pBt->fd, pNode, PAGE_SIZE, iPage*PAGE_SIZE);
	}
find_finish:
    free(pNode);
    return rc;
}
/*
 * 将结点分裂成2半
 */
void addNewSub(Node *p,Node *pNew,int median)
{
	BtreeRecord *pDstBuf,*pSrcBuf;
	int iAmt;

	//为方便起见，p->nodeHdr.nRecord定为奇数
	assert( (p->nodeHdr.nRecord & 0x01)!=0 );
	p->nodeHdr.nRecord /= 2;
	assert( p->nodeHdr.nRecord >= ((MAX_SUB_NUM+1)/2-1) );
	memcpy(&pNew->nodeHdr,&p->nodeHdr,sizeof(NodeHdr));

    pDstBuf = &pNew->aRecord[0];
    pSrcBuf = &p->aRecord[median+1];
    iAmt = (pNew->nodeHdr.nRecord+1)*sizeof(BtreeRecord);
    memcpy(pDstBuf,pSrcBuf,iAmt);

    memset(&p->aRecord[median],0,sizeof(Record));
}
/*
 * 结点数量超限后进行分裂
 * 最中间的记录上移到父节点
 * 然后剩下的结点分裂成2半
 */
void BtreeSplite(Btree *pBt, Node *p, Node *pParent,int iOffset)
{
	int median = (p->nodeHdr.nRecord-1)/2;
	BtreeRecord *pDstBuf,*pSrcBuf;
	int iAmt;
	Node *pNew = (Node*)malloc(PAGE_SIZE);
	u32 tmp1,tmp2;
	int szRecord = sizeof(BtreeRecord);

	memset(pNew,0,PAGE_SIZE);
    //如果分裂的是根结点，则需要再新增一个父节点作为根结点
	if(p->nodeHdr.isRoot)
	{
		memset(pParent,0,PAGE_SIZE);
        memcpy(&pParent->nodeHdr,&p->nodeHdr,sizeof(NodeHdr));
        pParent->nodeHdr.isLeaf = 0;
        pParent->nodeHdr.nRecord = 1;
        pDstBuf = &pParent->aRecord[0];
        pSrcBuf = &p->aRecord[median];
        memcpy(pDstBuf,pSrcBuf,sizeof(Record));
        if( pBt->nFree>1 )
        {
        	pParent->aRecord[0].iSub = popFreeSlot(pBt);
        	pParent->aRecord[1].iSub = popFreeSlot(pBt);
        }
        else
        {
			pParent->aRecord[0].iSub = pParent->nodeHdr.nPage++;
			pParent->aRecord[1].iSub = pParent->nodeHdr.nPage++;
			log_b("new page %d %d",pParent->aRecord[0].iSub,
					pParent->aRecord[1].iSub);
        }
        tmp1 = pParent->aRecord[0].iSub;
        tmp2 = pParent->aRecord[1].iSub;
        memcpy(pBt->pRoot, pParent, PAGE_SIZE);
        p->nodeHdr.isRoot = 0;
        addNewSub(p, pNew, median);

        OsWrite(pBt->fd,pBt->pRoot,PAGE_SIZE,0);
        OsWrite(pBt->fd,p,PAGE_SIZE,pParent->aRecord[0].iSub*PAGE_SIZE);
        OsWrite(pBt->fd,pNew,PAGE_SIZE,pParent->aRecord[1].iSub*PAGE_SIZE);
	}
	else
	{
        pDstBuf = &pParent->aRecord[iOffset+1];
        pSrcBuf = &pParent->aRecord[iOffset];
        iAmt = ((++pParent->nodeHdr.nRecord)-iOffset)*szRecord;
        memmove(pDstBuf,pSrcBuf,iAmt);
        memcpy(pSrcBuf, &p->aRecord[median], sizeof(Record));
        //assert( pDstBuf->key!=pSrcBuf->key );
        assert( pBt->iPage==pDstBuf->iSub );
        pSrcBuf->iSub = pBt->iPage;
        if( pBt->nFree>0 )
        {
        	pDstBuf->iSub = popFreeSlot(pBt);
        }
        else
        {
        	if(pParent->nodeHdr.isRoot)
        	{
        		pDstBuf->iSub = pParent->nodeHdr.nPage++;
        	}
        	else
        	{
				pDstBuf->iSub = pBt->pRoot->nodeHdr.nPage++;
				OsWrite(pBt->fd,pBt->pRoot,PAGE_SIZE,0);
        	}
        	log_b("new page %d",pDstBuf->iSub);

        }

        addNewSub(p, pNew, median);

        if(pParent->nodeHdr.isRoot)
        {
        	memcpy(pBt->pRoot, pParent, PAGE_SIZE);
        }
        OsWrite(pBt->fd,pParent,PAGE_SIZE,pBt->iParent*PAGE_SIZE);
        OsWrite(pBt->fd,p,PAGE_SIZE,pSrcBuf->iSub*PAGE_SIZE);
        OsWrite(pBt->fd,pNew,PAGE_SIZE,pDstBuf->iSub*PAGE_SIZE);
        tmp1 = pSrcBuf->iSub;
        tmp2 = pDstBuf->iSub;
	}
	pBt->iPage = tmp1;
	assert(pBt->iPage);
	if( (pBt->iRecord>median) )
	{
		memcpy(p,pNew,PAGE_SIZE);
		pBt->iPage = tmp2;
	}
    free(pNew);
}

void BtreeInsert(Btree *pBt, int key)
{
	Node *pNode =(Node *)malloc(PAGE_SIZE);
	Node *pParent = (Node*)malloc(PAGE_SIZE);
	NodeHdr *pHdr;
	int iPage = 0;
	int iOffset = 0;
	BtreeRecord *pDstBuf,*pSrcBuf;
	int iAmt;

	memcpy(pNode, pBt->pRoot, PAGE_SIZE);

	while( 1 )
	{
		pHdr = &pNode->nodeHdr;

		if( searchRecord(pBt,pNode,key,iPage) )
		{
			log_a("collision %d",key);
			break;
		}
		if( pHdr->nRecord == MAX_SUB_NUM-1 )
		{
			assert( iPage==pBt->iPage );
			log_b("split %d",iPage);
			BtreeSplite(pBt,pNode,pParent,iOffset);

			if( pParent->nodeHdr.isRoot )
			{
				assert( memcmp(pParent,pBt->pRoot,PAGE_SIZE)==0 );
			}
			assert( !pNode->nodeHdr.isRoot );
            //分裂后获得pNode所在的子结点位置
			//接续查找插入pNode所在的叶子结点
			//遇到结点数量超限后继续分裂
			iPage = pBt->iPage;
            continue;
		}
		//找到叶子结点后插入数据
		if(pHdr->isLeaf)
		{
			pDstBuf = &pNode->aRecord[pBt->iRecord+1];
			pSrcBuf = &pNode->aRecord[pBt->iRecord];
			iAmt = (++pHdr->nRecord - pBt->iRecord)*sizeof(BtreeRecord);
			if( iPage==pBt->pRoot->nodeHdr.nPage-1 )
			{
				pBt->mxRecord = pHdr->nRecord;
			}
			memmove(pDstBuf,pSrcBuf,iAmt);
			pNode->aRecord[pBt->iRecord].key = key;
			memcpy(pNode->aRecord[pBt->iRecord].aMagic, aMagic, 4);
			for(int i=0; i<8; i++)
			{
				pNode->aRecord[pBt->iRecord].aData[i] = i;
			}
			//assert( pDstBuf->key!=pSrcBuf->key );
			//该函数与OsWrite绑定在一起，确保根节点和磁盘中的数据保持同步
			updateRoot(pBt,pNode);
			OsWrite(pBt->fd, pNode, PAGE_SIZE, iPage*PAGE_SIZE);

			break;
		}
		pBt->iParent = pBt->iPage;
		assert( iPage==pBt->iParent );
		//把iPage更新为子结点地址
		iPage = pBt->record.iSub;
		iOffset = pBt->iRecord;
		memcpy(pParent, pNode, PAGE_SIZE);
		OsRead(pBt->fd, pNode, PAGE_SIZE, iPage*PAGE_SIZE);
	}

    free(pNode);
    free(pParent);
}
/*
 * p结点关键子数量少于最小值，把父节点的关键子
 * 移到p结点少，再把相邻结点的关键字上移到父结点
 * 根据邻居结点在父结点的左边还是右边，处理情况有所不同
 */
void getNeighborKey(
		Node *p,
		Node *pNbr,
		Node *pParent,
		int flag,
		int iOffset)
{
	log_fun("%s",__FUNCTION__ );
	BtreeRecord *pDstBuf,*pSrcBuf;
	int iAmt;
	if(flag==RIGHT_NODE)
	{
		pDstBuf = &p->aRecord[p->nodeHdr.nRecord++];
		pSrcBuf = &pParent->aRecord[iOffset];
	}
	else
	{
		memmove(&p->aRecord[1],&p->aRecord[0],
				(++p->nodeHdr.nRecord)*sizeof(BtreeRecord));
		pDstBuf = &p->aRecord[0];
		pSrcBuf = &pParent->aRecord[iOffset];
	}
	memcpy(pDstBuf,pSrcBuf,sizeof(Record));
	//assert( p->aRecord[1].key!=p->aRecord[0].key );

	if(flag==RIGHT_NODE)
	{
		pSrcBuf = &pNbr->aRecord[0];
		(pDstBuf+1)->iSub = pSrcBuf->iSub;
	}
	else
	{
		pSrcBuf = &pNbr->aRecord[--pNbr->nodeHdr.nRecord];
		pDstBuf->iSub = (pSrcBuf+1)->iSub;
	}
	pDstBuf = &pParent->aRecord[iOffset];
	memcpy(pDstBuf,pSrcBuf,sizeof(Record));


	if(flag==RIGHT_NODE)
	{
		pDstBuf = &pNbr->aRecord[0];
		pSrcBuf = &pNbr->aRecord[1];
		iAmt = (pNbr->nodeHdr.nRecord--)*sizeof(BtreeRecord);
		memmove(pDstBuf,pSrcBuf,iAmt);
		//assert( pDstBuf->key!=pSrcBuf->key );
	}
	else
	{
		memset(&pNbr->aRecord[pNbr->nodeHdr.nRecord],0,sizeof(Record));
	}
}
/*
 * 当邻居结点的关键字数量也到了最小值时
 * 父结点的关键子与左右结点合并，删除右边的结点
 */
u32 mergeBtree(
		Node *p,
		Node *pNbr,
		Node *pParent,
		int iOffset)
{
	log_fun("%s",__FUNCTION__ );
	u32 iDelete;
	BtreeRecord *pDstBuf,*pSrcBuf;
	int iAmt;
	int szRecord = sizeof(BtreeRecord);

	pDstBuf = &p->aRecord[p->nodeHdr.nRecord];
	pSrcBuf = &pParent->aRecord[iOffset];
	memcpy(pDstBuf,pSrcBuf,sizeof(Record));

	pDstBuf = &pParent->aRecord[iOffset];
	pSrcBuf = &pParent->aRecord[iOffset+1];
	iDelete = pSrcBuf->iSub;
	log_b("delete %d get %d",pSrcBuf->iSub,pDstBuf->iSub);
	pSrcBuf->iSub = pDstBuf->iSub;
	iAmt = ((pParent->nodeHdr.nRecord--)-iOffset)*szRecord;
	memmove(pDstBuf,pSrcBuf,iAmt);
	log_b("next %d now %d",pSrcBuf->iSub,pDstBuf->iSub);
	//assert( pDstBuf->key!=pSrcBuf->key );
	pDstBuf = &p->aRecord[++p->nodeHdr.nRecord];
	pSrcBuf = &pNbr->aRecord[0];
	iAmt = (pNbr->nodeHdr.nRecord+1)*szRecord;
	p->nodeHdr.nRecord += pNbr->nodeHdr.nRecord;
	assert( p->nodeHdr.nRecord<MAX_SUB_NUM );
	memcpy(pDstBuf,pSrcBuf,iAmt);
	memset(pNbr, 0, PAGE_SIZE);
	log_b("delete page %d",iDelete);
	return iDelete;
}
/*
 * 结点关键字数量小于最小值时，要进行调整从而保持平衡
 */
void adjustBtree(Btree *pBt, Node *p, Node *pParent, u32 iPage)
{
	log_fun("%s",__FUNCTION__ );
	Node *pRight = (Node*)malloc(PAGE_SIZE);
	Node *pLeft = (Node*)malloc(PAGE_SIZE);
	int i;
	int iLeft,iRight;
	int flag = 0;

	int iOffset;
	u32 iDelete = 0;

	assert( !p->nodeHdr.isRoot );
	pBt->iParent = pBt->aFind[--pBt->iFind];
	OsRead(pBt->fd, pParent, PAGE_SIZE, pBt->iParent*PAGE_SIZE);
	for(i=0; i<=pParent->nodeHdr.nRecord; i++)
	{
		if( pParent->aRecord[i].iSub==iPage )
		{
			iOffset = i;
			break;
		}
	}
    assert( i<=pParent->nodeHdr.nRecord );
	if( iOffset<pParent->nodeHdr.nRecord )
	{
		iRight = pParent->aRecord[iOffset+1].iSub;
		OsRead(pBt->fd, pRight, PAGE_SIZE, iRight*PAGE_SIZE);

		if( pRight->nodeHdr.nRecord>(MAX_SUB_NUM/2 - 1) )
		{
			getNeighborKey(p,pRight,pParent,RIGHT_NODE,iOffset);
		}
		else if( iOffset>0 )
		{
			iLeft = pParent->aRecord[iOffset-1].iSub;
			OsRead(pBt->fd, pLeft, PAGE_SIZE, iLeft*PAGE_SIZE);

			if( pLeft->nodeHdr.nRecord>(MAX_SUB_NUM/2 - 1) )
			{
				getNeighborKey(p,pLeft,pParent,LEFT_NODE,iOffset-1);
				flag = 1;
			}
			else
			{
				iDelete = mergeBtree(p,pRight,pParent,iOffset);
			}
		}
		else
		{
			iDelete = mergeBtree(p,pRight,pParent,iOffset);
		}
	}
	else
	{
		iLeft = pParent->aRecord[iOffset-1].iSub;
		OsRead(pBt->fd, pLeft, PAGE_SIZE, iLeft*PAGE_SIZE);

		if( pLeft->nodeHdr.nRecord>((MAX_SUB_NUM+1)/2 - 1) )
		{
			getNeighborKey(p,pLeft,pParent,LEFT_NODE,iOffset-1);
		}
		else
		{
			iDelete = mergeBtree(pLeft,p,pParent,iOffset-1);
		}
		flag = 1;
	}
	/////////////////////////////////////
	if(flag)
	{
		assert( iLeft!=0 );
		OsWrite(pBt->fd, pLeft, PAGE_SIZE, iLeft*PAGE_SIZE);
	}
	else
	{
		assert( iRight!=0 );
		OsWrite(pBt->fd, pRight, PAGE_SIZE, iRight*PAGE_SIZE);
	}
	updateRoot(pBt, pParent);
	OsWrite(pBt->fd, pParent, PAGE_SIZE, pBt->iParent*PAGE_SIZE);
	OsWrite(pBt->fd, p, PAGE_SIZE, iPage*PAGE_SIZE);
	///////////////////////////////
	assert( iDelete<pBt->pRoot->nodeHdr.nPage );
	if( iDelete==pBt->pRoot->nodeHdr.nPage-1 )
	{
		pBt->pRoot->nodeHdr.nPage--;
		OsWrite(pBt->fd, pBt->pRoot, PAGE_SIZE, 0);
	}
	else
	{
		addFreeSlot(pBt,iDelete);
	}
	free(pLeft);
	free(pRight);
}
/*
 * 删除叶子结点中偏移位置为pBt->iRecord的记录
 */
void BtreeDeleteLeaf(Btree *pBt, Node *p)
{
	BtreeRecord *pDstBuf,*pSrcBuf;
	int iAmt;

	pDstBuf = &p->aRecord[pBt->iRecord];
	pSrcBuf = &p->aRecord[pBt->iRecord+1];
	assert(p->nodeHdr.nRecord>pBt->iRecord);
	log_a("Record %d %d",pBt->iRecord,p->nodeHdr.nRecord);
	assert( (pDstBuf->key!=pSrcBuf->key)
			||p->nodeHdr.nRecord==pBt->iRecord+1);
	iAmt = ((p->nodeHdr.nRecord--)-pBt->iRecord)*sizeof(BtreeRecord);

	memmove(pDstBuf,pSrcBuf,iAmt);

	OsWrite(pBt->fd, p, PAGE_SIZE, pBt->iPage*PAGE_SIZE);
    if(p->nodeHdr.isRoot)
    {
    	memcpy(pBt->pRoot, p, PAGE_SIZE);
    }
}
/*
 * 要删除的记录不在叶子结点内，需要在向下查找
 * 到叶子结点中与之相邻的关键字，将其上移到要删除的位置
 * 还要记录搜索路径，因为如果叶子结点的关键字数量小于最小值后还要调整
 */
int BtreeDeleteInside(Btree *pBt, Node *p)
{
	log_fun("%s",__FUNCTION__ );
	BtreeRecord *pDstBuf,*pSrcBuf;
	int iAmt;
	int iLeft,iRight;
	Node *pRight = (Node*)malloc(PAGE_SIZE);
	Node *pLeft = (Node*)malloc(PAGE_SIZE);
	int iPage = pBt->iPage;
	u32 iRecord = pBt->iRecord;
	u32 iDelete;
	int rc = 0;

	iLeft = p->aRecord[iRecord].iSub;
	OsRead(pBt->fd, pLeft, PAGE_SIZE, iLeft*PAGE_SIZE);
	while(!pLeft->nodeHdr.isLeaf)
	{
		iLeft = pLeft->aRecord[pLeft->nodeHdr.nRecord].iSub;
		OsRead(pBt->fd, pLeft, PAGE_SIZE, iLeft*PAGE_SIZE);
	}
	if( pLeft->nodeHdr.nRecord>(MAX_SUB_NUM/2-1) )
	{
		pSrcBuf = &pLeft->aRecord[--pLeft->nodeHdr.nRecord];
		pDstBuf = &p->aRecord[iRecord];
		memcpy(pDstBuf,pSrcBuf,sizeof(Record));
		updateRoot(pBt,p);
		OsWrite(pBt->fd, p, PAGE_SIZE, iPage*PAGE_SIZE);
		OsWrite(pBt->fd, pLeft, PAGE_SIZE, iLeft*PAGE_SIZE);
		rc = 1;

	}
	else
	{
		iRight = p->aRecord[iRecord+1].iSub;
		pBt->aFind[pBt->iFind++] = iRight;
		OsRead(pBt->fd, pRight, PAGE_SIZE, iRight*PAGE_SIZE);
		while(!pRight->nodeHdr.isLeaf)
		{
			iRight = pRight->aRecord[0].iSub;
			pBt->aFind[pBt->iFind++] = iRight;
			OsRead(pBt->fd, pRight, PAGE_SIZE, iRight*PAGE_SIZE);
		}

		//此时即使结点少于规定也上移
		//if( pRight->nodeHdr.nRecord>(MAX_SUB_NUM/2-1) )
		{
			pSrcBuf = &pRight->aRecord[0];
			pDstBuf = &p->aRecord[iRecord];
			memcpy(pDstBuf,pSrcBuf,sizeof(Record));
			updateRoot(pBt,p);
			OsWrite(pBt->fd, p, PAGE_SIZE, iPage*PAGE_SIZE);
			pSrcBuf = &pRight->aRecord[1];
			pDstBuf = &pRight->aRecord[0];
			iAmt = (pRight->nodeHdr.nRecord--)*sizeof(BtreeRecord);
			memmove(pDstBuf,pSrcBuf,iAmt);
			//assert( pDstBuf->key!=pSrcBuf->key );
			OsWrite(pBt->fd, pRight, PAGE_SIZE, iRight*PAGE_SIZE);
		}
		if( pRight->nodeHdr.nRecord>=((MAX_SUB_NUM+1)/2-1) )
		{
			rc = 1;
		}
		else  //结点已经小于最小值,需要再做调整
		{
			memcpy(p,pRight,PAGE_SIZE);
			pBt->iPage = iRight;
		}
	}


	free(pLeft);
	free(pRight);
	return rc;
}

//从叶子结点开始，一直向上调整，直到到达根结点为止
void BtreeAdjust(Btree *pBt, Node* pNode,Node *pParent)
{
	u32 iPage;


	memcpy(pParent, pNode, PAGE_SIZE);
	assert( pBt->iFind>0 );
	iPage = pBt->aFind[--pBt->iFind];
	while(pParent->nodeHdr.nRecord<(MAX_SUB_NUM/2 - 1))
	{
		if( pParent->nodeHdr.isRoot )
		{
			assert( pBt->iFind==0 );
			break;
		}
		memcpy(pNode, pParent, PAGE_SIZE);
		adjustBtree(pBt, pNode, pParent, iPage);
		iPage = pBt->iParent;
	}

}
void BtreeDelete(Btree *pBt, int key)
{
	Node *pNode = (Node *)malloc(PAGE_SIZE);
	Node *pParent = (Node*)malloc(PAGE_SIZE);
	u32 iPage;

	pBt->pNode = pNode;
	pBt->getFlag = 1;
	if( BtreeFind(pBt, key) )
	{
		log_a("delete key %d page %d offset %d",key,pBt->iPage,pBt->iRecord);
		if( pNode->nodeHdr.isLeaf )
		{
			BtreeDeleteLeaf(pBt, pNode);
			BtreeAdjust(pBt, pNode, pParent);
		}
		else
		{
			if( !BtreeDeleteInside(pBt,pNode) )
			{
				//删除中间记录后，叶子结点的记录不满足最小值
				//此时已经把靠近key的叶子结点移到了key的位置上
				//pNode更新为叶子结点
				BtreeAdjust(pBt, pNode, pParent);
			}
		}
	}
	else
	{
		log_a("cannt find delete %d",key);
	}

	pBt->getFlag = 0;
    free(pNode);
    free(pParent);
}

typedef struct{
	int sum;
	int key;
	int flag;

}TestData;
TestData aData[100];
/*
 * 测试历史插入数据能否在Btree中查找到
 */
void test_case(Btree *pBt, int key ,int flag)
{
	static int k=0;
	static int i=0;
	static int isInit = 0;
	static int sum = 0;
	BtreeRecord temp;
	int j;

	if(!isInit)
	{
		memset(aData,0,sizeof(aData));
		isInit = 1;
	}
	if(flag)//添加
	{
		sum++;
		k++;
		if( k>random_()%10 )
		{
			k = 0;
			if(i<100)
			{
				i++;
				aData[i].key = key;
				aData[i].sum = sum;
				aData[i].flag = flag;
			}
			else
			{
				i = 0;
			}
		}
	}

	for(j=0;j<100;j++)
	{
		if(!flag)
		{
			if(aData[j].key == key)
			{
				aData[j].flag = flag;
			}
		}

	}

	if(i==20)
	{
		for(j=0;j<20;j++)
		{
			if(aData[j].flag)
			{
				log_a("want to find abuf[%d] %d",j,aData[j].key);
				log_a("insert %d now %d",aData[j].sum,sum);
				assert( BtreeFind(pBt,aData[j].key) );
				log_a("find page %d",pBt->iPage);
				OsRead(pBt->fd,&temp,sizeof(BtreeRecord),
						pBt->iPage*PAGE_SIZE+16+
						pBt->iRecord*sizeof(BtreeRecord));
				assert(temp.key==aData[j].key);
			}
		}

	}
}

void btree_test(Btree *pBt)
{
	int key;
	int i,j;
	BtreeRecord record;
	int szRecord = sizeof(BtreeRecord);
	int iOffset;
	u32 maxRecord;

	for(i=0; i<10000; i++)
	{
		for(j =0;j<10;j++)
		{
			key = random_();
			log_a("want to insert key %d",key);
			BtreeInsert(pBt,key);
			//assert(BtreeFind(pBt,key));
			//test_case(pBt,key,1);
			//log_a("find page %d offset %d",pBt->iPage,pBt->iRecord);
		}

		for(j =0;j<100;j++)
		{
			maxRecord = (pBt->pRoot->nodeHdr.nPage-1)*
					(PAGE_SIZE-sizeof(NodeHdr))/szRecord+pBt->mxRecord;
			int tmp = random_()%maxRecord;
			iOffset = tmp*szRecord+(tmp/MAX_SUB_NUM+1)*sizeof(NodeHdr);
			OsRead(pBt->fd, &record, szRecord, iOffset);
			key = record.key;
			log_a("want to find %d key %d",iOffset,key);
			BtreeFind(pBt,key);
		}

		for(j=0;j<2;j++)
		{
			maxRecord = (pBt->pRoot->nodeHdr.nPage-1)*
					(PAGE_SIZE-sizeof(NodeHdr))/szRecord+pBt->mxRecord;
			int tmp = random_()%maxRecord;
			iOffset = tmp*szRecord+(tmp/MAX_SUB_NUM+1)*sizeof(NodeHdr);
			OsRead(pBt->fd, &record, szRecord, iOffset);
			key = record.key;
			log_a("want to delete %d",iOffset);
			BtreeDelete(pBt,key);

			//assert( !BtreeFind(pBt,key) );
//			test_case(pBt,key,0);
		}
	}
}

void BtreeTest(void)
{
	char *zFileName = "testdata";
	Btree *pBt = OpenBtree(zFileName);
	time_t t1,t2;
	t1 = time(NULL);
	btree_test(pBt);
	t2 = time(NULL);
	printf("time %ld\n",t2-t1);
    CloseBtree(pBt);
	//remove(zFileName);
}
