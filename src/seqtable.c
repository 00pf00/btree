#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include "test.h"

#define BUF_SIZE 10
#define BLOCK_NUM 10
#define KEY_SIZE 1000000
#define PAGE_SIZE 4096
#define PAGE_SLOT (PAGE_SIZE/sizeof(Record))

typedef struct SeqTable SeqTable;

struct SeqTable
{
	int fd;
	u16 maxPage;
	u16 pageSize;
	int freeSlot;//指向空闲页或文件末尾
	int nFreeSlot;
	u32 iRecord;
	u32 maxRecord;
	Record *pRecord;
	u8 iBufOffst;
	//写满BUF_SIZE个记录后再写入磁盘
	Record insert_buf[BUF_SIZE];
	Bitvec **paBlock;//指向每个block的bitmap
	u32 nBlock;
};

int SeqFind(SeqTable *pSeqTable, int key);
int OsRead(int fd, void *zBuf, int iAmt, long iOfst);
int OsWrite(int fd, void *zBuf, int iAmt, long iOfst);

u32 random_(void)
{
	static u32 x = 0;
	static u8 isInit = 0;
	if(!isInit)
	{
		isInit =1;
		int t = (unsigned int)time(NULL);
		printf("t %d\n",t);
		srand(t);
	}
    for(int i=0; i<20; i++)
    {
    	x ^= (rand()&1)<<i;
    }
    x++;
    return x%KEY_SIZE;
}

void writeData(Record *pBuf, int key)
{
	int i;
	pBuf->key = key;
	memcpy(pBuf->aMagic, aMagic, 4);
	for(i=0; i<8; i++)
	{
		pBuf->aData[i] = i;
	}
}



int SequenceFind(SeqTable *pSeqTable, int key)
{
	int i,j;
	int cnt = 0;
	int szRecord = sizeof(Record);
	u16 szPage = pSeqTable->pageSize;
	int iOffset;
	u8 aBuf[PAGE_SIZE];
	Record *pBuf;
	int fd = pSeqTable->fd;

	for(i=0; i<=pSeqTable->maxPage; i++)
	{
		iOffset = i*szPage;
		OsRead(fd, aBuf, szPage, iOffset);
		for(j=0; j<PAGE_SLOT; j++)
		{
			if(++cnt > pSeqTable->maxRecord)
			{
				return 0;
			}
			pBuf = (Record*)(&aBuf[j*szRecord]);
			if(memcmp(pBuf->aMagic, aMagic, 4) == 0)
			{
				if(pBuf->key == key)
				{
					pSeqTable->iRecord = i*PAGE_SLOT+j;
					memcpy(pSeqTable->pRecord,pBuf,szRecord);
					//printf("find:%d\n",i*256+j);
					return 1;
				}
			}
			else if(pSeqTable->freeSlot<0)
			{
				//freeSlot=-1表示当前为空闲slot
				//找到空闲的slot，将来直接把数据插入空闲slot
				pSeqTable->freeSlot = i*PAGE_SLOT+j;
				//printf("getfree %d\n",pSeqTable->freeSlot*szRecord);
			}
		}
	}

	return 0;
}

void SequenceInsert(SeqTable *pSeqTable, int key)
{
	Record record;
	int iOffset;
	int szRecord = sizeof(Record);
	if(!SequenceFind(pSeqTable,key))
	{
		writeData(&record, key);
	    if(pSeqTable->freeSlot>=0)
	    {
	    	iOffset = pSeqTable->freeSlot*szRecord;
	    	pSeqTable->freeSlot = -1;
	    }
	    else  //没有空闲slot时把数据插入到文件末尾
	    {
	    	iOffset = pSeqTable->maxRecord*szRecord;
	    	pSeqTable->maxRecord++;
	    	pSeqTable->maxPage = (iOffset+szRecord)/4096;
	    }
	    OsWrite(pSeqTable->fd,&record,szRecord,iOffset);;
	}
	else
	{
		//printf("collision\n");
	}

}
void SequenceDelete(SeqTable *pSeqTable, int key)
{
	u8 aBuf[16] = {0};
	int iOffset;
	int szRecord = sizeof(Record);
	if(SequenceFind(pSeqTable,key))
	{
		iOffset = pSeqTable->iRecord*szRecord;
		OsWrite(pSeqTable->fd,aBuf,szRecord,iOffset);
		pSeqTable->freeSlot = pSeqTable->iRecord;
		//printf("delete %d\n",iOffset);
	}
}

void SeqTest1(SeqTable *pSeqTable)
{
	int key;
	int i,j;
	Record record;
	int szRecord = sizeof(Record);
	int iOffset;

	for(i=0; i<1000; i++)
	{
		for(j =0;j<10;j++)
		{
			key = random_();
			SequenceInsert(pSeqTable,key);
			//assert(SequenceFind(pSeqTable,key));
		}
		for(j =0;j<100;j++)
		{
			iOffset = (random_()%pSeqTable->maxRecord)*szRecord;
			OsRead(pSeqTable->fd, &record, szRecord, iOffset);
			key = record.key;
			SequenceFind(pSeqTable,key);
		}
		for(j=0;j<2;j++)
		{
			iOffset = (random_()%pSeqTable->maxRecord)*szRecord;
			OsRead(pSeqTable->fd, &record, szRecord, iOffset);
			key = record.key;
			//printf("want to delete %d\n",iOffset);
			SequenceDelete(pSeqTable,key);
			//assert(!SequenceFind(pSeqTable,key));
		}
	}
}
SeqTable *OpenSeqTable(char *zName)
{
	remove(zName);
	SeqTable *pSeqTable = (SeqTable*)malloc(sizeof(SeqTable));
	memset(pSeqTable,0,sizeof(SeqTable));
	pSeqTable->pageSize = PAGE_SIZE;
	pSeqTable->freeSlot = -1;
	pSeqTable->pRecord = (Record*)malloc(sizeof(Record));
	pSeqTable->paBlock = (Bitvec**)malloc(sizeof(Bitvec*));
	pSeqTable->paBlock[0] = sqlite3BitvecCreate(KEY_SIZE);
	pSeqTable->nBlock++;
	pSeqTable->fd = open(zName, O_RDWR|O_CREAT, 0600);
	return pSeqTable;
}

void CloseSeqTable(SeqTable *pSeqTable)
{
	int i;
	for( i=0; i<pSeqTable->nBlock; i++)
	{
		sqlite3BitvecDestroy(pSeqTable->paBlock[i]);
	}
	close(pSeqTable->fd);
	free(pSeqTable->paBlock);
	free(pSeqTable->pRecord);
	free(pSeqTable);
}


//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
/*
 * 变量每一页找到空闲页slot，将末尾的记录移到空闲slot
 */
void SeqMoveSlot(SeqTable *pSeq)
{
	int fd = pSeq->fd;
	Record aBuf[PAGE_SIZE/sizeof(Record)];
	Record aMaxPageBuf[PAGE_SIZE/sizeof(Record)];
	u16 szPage = pSeq->pageSize;
	int szRecord = sizeof(Record);
	int iOfst;
	int i,j;
	Record *pBuf,*tmp;
	u8 aTmpSpace[512];
	int endOffset = 0;
	int iBlock,nBlock;
	int mxOffset;
	int dstOffset,srcOffset;
	log_b("maxPage %d",pSeq->maxPage);
	log_c("begin move");
	for(i=0; i<=pSeq->maxPage; i++)
	{

		iOfst = i*szPage;
		OsRead(fd, aBuf, szPage, iOfst);

		for(j=0; j<256; j++)
		{
			dstOffset = iOfst+j*szRecord;
			pBuf = &aBuf[j];
			//前往后遍历空闲slot
			if(memcmp(pBuf->aMagic,aMagic,4)!=0)
			{
				//读取尾页数据
				if( endOffset==0 )
				{
					mxOffset = pSeq->maxPage*szPage;
					OsRead(fd, aMaxPageBuf, szPage, mxOffset);
					endOffset = pSeq->maxRecord*szRecord-mxOffset;
					assert( endOffset>=0 && endOffset<=PAGE_SIZE );
				}
				//从后往前直到找到非空slot
				while( endOffset>0 )
				{
					endOffset -= szRecord;
					srcOffset = mxOffset+endOffset;
					if( srcOffset<=dstOffset )
					{
						//开始地址大于结尾地址，移动结束
						log_b("now Page %d",pSeq->maxPage);
						log_b("src %d dst %d",srcOffset,dstOffset);
						log_b("free slot %d",pSeq->nFreeSlot);
						return;
					}
					assert(pBuf->key==0);
					assert(endOffset%szRecord==0);
					tmp = &aMaxPageBuf[endOffset/szRecord];
					if( memcmp(tmp->aMagic,aMagic,4)==0 )
					{
						//把末尾的记录移到前面空闲的slot
						OsWrite(fd, tmp, szRecord, dstOffset);
						nBlock = srcOffset/(szPage*BLOCK_NUM);

						//assert(SeqFind(pSeq,tmp->key));
						assert( sqlite3BitvecTest(pSeq->paBlock[nBlock], tmp->key) != 0 );
						//清除当前记录所在块的bitmap
						sqlite3BitvecClear(pSeq->paBlock[nBlock], tmp->key, aTmpSpace);
						//assert(!SeqFind(pSeq,tmp->key));
						assert( sqlite3BitvecTest(pSeq->paBlock[nBlock], tmp->key) == 0 );
						//设置记录在目标地址所在块的bitmap
						iBlock = (dstOffset)/(szPage*BLOCK_NUM);
						sqlite3BitvecSet(pSeq->paBlock[iBlock], tmp->key);

						log_c("%d move %d to %d",pSeq->nFreeSlot,srcOffset,dstOffset);

						log_a("move %d to %d",srcOffset,dstOffset);
						log_a("nFreeSlot %d maxRecord %d maxPage%d dst %d",
								pSeq->nFreeSlot,pSeq->maxRecord,pSeq->maxPage,dstOffset);
						//更新相关信息
						pSeq->maxRecord = srcOffset/szRecord;
						pSeq->maxPage = (srcOffset-szRecord)/szPage;
						pSeq->nBlock = pSeq->maxPage/BLOCK_NUM + 1;
						pSeq->nFreeSlot--;
						//assert(SeqFind(pSeq,tmp->key));

						break;
					}
					else
					{
						//此时遇到了空闲slot
						log_c("%d not move %d to %d",pSeq->nFreeSlot,srcOffset,dstOffset);
						assert(tmp->key==0);
						pSeq->maxRecord = srcOffset/szRecord;
						pSeq->maxPage = (srcOffset-szRecord)/szPage;
						pSeq->nBlock = pSeq->maxPage/BLOCK_NUM + 1;
						pSeq->nFreeSlot--;
					}

					assert(srcOffset>dstOffset);
				}
			}
//			if(pSeq->nFreeSlot<0)
//			{
//				log_b("now Page %d",pSeq->maxPage);
//				log_b("src %d dst %d",srcOffset,dstOffset);
//			}

			assert( pSeq->nFreeSlot>=0 );
		}
	}
	log_c("move end %d",pSeq->nFreeSlot);
}

void SeqInsertFlush(SeqTable *pSeq)
{
	Record *paBuf = pSeq->insert_buf;
	int szRecord = sizeof(Record);
	int iAmt = pSeq->iBufOffst*szRecord;
	int iOfst = pSeq->maxRecord*szRecord;
	int i;
	u16 szPage = pSeq->pageSize;
	int fd = pSeq->fd;
	int maxPage = pSeq->maxPage;
    int nBlock;
    Bitvec **paNew = pSeq->paBlock;
    int iBlock = pSeq->nBlock;
    int key = 0;

    log_a("flush %d %d",iOfst,iAmt);

	OsWrite(fd,paBuf,iAmt,iOfst);

	pSeq->maxRecord += pSeq->iBufOffst;
	pSeq->maxPage = (iOfst+iAmt)/szPage;
	log_a("maxPage %d offset %d",pSeq->maxPage,iOfst+iAmt);
	//每新增10页添加一个新块
	if( pSeq->maxPage%BLOCK_NUM==0 && pSeq->maxPage>maxPage )
	{
		nBlock = pSeq->maxPage/BLOCK_NUM+1;
		log_a("nBlock %d iBlock %d",nBlock,iBlock);
		if( nBlock>iBlock)
		{
			paNew = (Bitvec**)realloc(pSeq->paBlock, (nBlock)*sizeof(Bitvec*));
			memset(&paNew[iBlock], 0, (nBlock-iBlock)*sizeof(Bitvec*));
			pSeq->nBlock = nBlock;
			for(i=iBlock; i<nBlock; i++)
			{
				paNew[i] = sqlite3BitvecCreate(KEY_SIZE);
			}
			pSeq->paBlock = paNew;
		}
	}
	//缓存写入磁盘后设置每个记录所在块的bitmap
	for(i=0; i<pSeq->iBufOffst; i++)
	{
		iBlock = (iOfst + i*szRecord)/(szPage*BLOCK_NUM);
		key = paBuf[i].key;
		if(!pSeq->paBlock[iBlock])
		{
			log_a("iBlock %d",iBlock);
			log_a("iOfst %d i %d",iOfst,i);
			assert(pSeq->paBlock[iBlock]);
		}

		sqlite3BitvecSet(pSeq->paBlock[iBlock], key);

		log_a("setbit block %d key %d",iBlock,key);
		//assert( SeqFind(pSeq,key) );
	}

	assert(iAmt/szRecord==pSeq->iBufOffst);
	assert(iOfst+iAmt==pSeq->maxRecord*szRecord);

	//清空缓冲区
	memset(paBuf, 0 ,iAmt);
	pSeq->iBufOffst = 0;
	//////////////////////////////////////////
    //当空闲slot大于1000后开始填充空闲slot
    if( pSeq->nFreeSlot>1000 )
    {
    	SeqMoveSlot(pSeq);
    }
}

/*
 * 插入数据，可能和磁盘里的数据冲突也可能和缓存里的数据冲突
 */
void SeqInsertCache(SeqTable *pSeqTable, int key)
{
	int i;
	int iOffst = pSeqTable->iBufOffst;
	Record *pBuf = (Record *)&pSeqTable->insert_buf[iOffst];
	if(SeqFind(pSeqTable,key))
	{
		//冲突
		return;
		//log_a("actul offset %d",(pSeqTable->maxRecord)*sizeof(Record));
		//log_a("colision key %d record %d",key,pSeqTable->iRecord);
	}
	for(i=0; i<iOffst;i++)
	{
		if( key==pSeqTable->insert_buf[i].key )
		{
			//和缓存中的数据冲突
			return;
		}
	}
	assert( iOffst<BUF_SIZE );

	writeData(pBuf, key);

	log_a("insert %d ofst %d", key, pSeqTable->iBufOffst);
	pSeqTable->iBufOffst++;

	if( pSeqTable->iBufOffst==BUF_SIZE )
	{
		SeqInsertFlush(pSeqTable);
	}

}

int SeqFind(SeqTable *pSeqTable, int key)
{
	int i,j,k;
	int iOffset;
	int szPage = pSeqTable->pageSize;
	int szRecord = sizeof(Record);
	int fd = pSeqTable->fd;
	Record aBuf[PAGE_SIZE/sizeof(Record)];
	Record *pRecord;
	int nRecord = szPage/szRecord;

	for( i=0;i<pSeqTable->nBlock;i++ )
	{
		//按块查找
		if(sqlite3BitvecTest(pSeqTable->paBlock[i],key))
		{
			//只要进入这里说明要查找的记录在该磁盘块内
			for( k=0; k<10; k++)
			{
				iOffset = (i*BLOCK_NUM+k)*szPage;
				OsRead(fd, aBuf, szPage, iOffset);
				for(j=0; j<nRecord; j++)
				{
					if( (iOffset+j*szRecord)>=pSeqTable->maxRecord*szRecord )
					{
						assert(0);
						break;
					}

					pRecord = &aBuf[j];
					if( pRecord->key==key )
					{
						log_a("find %d iBlock %d",iOffset+j*szRecord,i);
						pSeqTable->iRecord = (iOffset+j*szRecord)/szRecord;
						memcpy(pSeqTable->pRecord,pRecord,szRecord);
						return 1;
					}

				}
			}
			assert(k<10);
		}
	}
	log_a("not find %d",key);
	return 0;

}
void SeqDelete(SeqTable *pSeqTable, int key)
{
	u8 aBuf[16] = {0};
	int iOffset;
	int szRecord = sizeof(Record);
	u8 aTmp[512];
	int iBlock;

	if( SeqFind(pSeqTable,key) )
	{
		//先查找要删除块的位置iRecord
		iOffset = pSeqTable->iRecord*szRecord;

		OsWrite(pSeqTable->fd,aBuf,szRecord,iOffset);
		pSeqTable->nFreeSlot++;
		//清除对应块的bitmap
		iBlock = iOffset/(pSeqTable->pageSize*BLOCK_NUM);
		sqlite3BitvecClear(pSeqTable->paBlock[iBlock],key,aTmp);

		//assert(!SeqFind(pSeqTable,key));
		log_a("delete %d %d",iBlock,iOffset);
		log_c("delete %d %d",pSeqTable->nFreeSlot,iOffset);
	}
}

void SeqTest2(SeqTable *pSeqTable)
{
	int key;
	int i,j;
	Record record;
	int szRecord = sizeof(Record);
	int iOffset;

	SeqFind(pSeqTable,1);
	for(i=0; i<10000; i++)
	{
		for(j =0;j<10;j++)
		{
			key = random_()+1;
			SeqInsertCache(pSeqTable,key);
		}

		for(j =0;j<100;j++)
		{
			assert( pSeqTable->maxRecord != 0 );
			iOffset = (random_()%pSeqTable->maxRecord)*szRecord;
			OsRead(pSeqTable->fd, &record, szRecord, iOffset);
			key = record.key;
			log_a("key %d",key);
			SeqFind(pSeqTable,key);
		}

		for(j=0;j<2;j++)
		{
			iOffset = (random_()%pSeqTable->maxRecord)*szRecord;
			OsRead(pSeqTable->fd, &record, szRecord, iOffset);
			key = record.key;
			log_a("want to delete %d",iOffset);
			SeqDelete(pSeqTable,key);
		}
	}
}

void SequenceTest(void)
{
	char *zFileName = "testdata";
	SeqTable *pSeqTable = OpenSeqTable(zFileName);
	time_t t1,t2;
	t1 = time(NULL);
	//SeqTest1(pSeqTable);
	SeqTest2(pSeqTable);
	t2 = time(NULL);
	printf("time %ld\n",t2-t1);
    CloseSeqTable(pSeqTable);
	//remove(zFileName);
}
