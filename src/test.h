#ifndef TEST_H
#define TEST_H
#include<stdio.h>

typedef unsigned char  u8;
typedef unsigned int   u32;
typedef unsigned short u16;

typedef struct Record Record;
struct Record
{
	int key;
	u8 aMagic[4];
	u8 aData[8];
};

typedef struct Bitvec Bitvec;

Bitvec *sqlite3BitvecCreate(u32);
int sqlite3BitvecTest(Bitvec*, u32);
int sqlite3BitvecTestNotNull(Bitvec*, u32);
int sqlite3BitvecSet(Bitvec*, u32);
void sqlite3BitvecClear(Bitvec*, u32, void*);
void sqlite3BitvecDestroy(Bitvec*);
u32 sqlite3BitvecSize(Bitvec*);

#define log_a(format,...)  //printf(format"\n",## __VA_ARGS__)
#define log_fun(format,...)  //printf(format"\n",## __VA_ARGS__)
#define log_b(format,...)  //printf(format"\n",## __VA_ARGS__)
#define log_c(format,...)  //printf(format"\n",## __VA_ARGS__)
//log_a("%s",__FILE__);

static const unsigned char aMagic[] = {
  0xd9, 0xd5, 0x05, 0xf9,
};

static void memout(u8 *pdata,u8 len)
{
	int i;
	for(i=0;i<len;i++)
	{
		printf("%02X ",*(pdata+i));
		if((i+1)%8==0)
		{
			printf("\n");
		}
	}
	printf("\n");
}

u32 random_(void);


#endif
