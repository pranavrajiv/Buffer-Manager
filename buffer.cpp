/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 
FrameId frno;

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table
   numBufs= bufs;
  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {


for (FrameId p = 0; p < numBufs; p++)
  {
        if(bufDescTable[p].dirty)
        {
	flushFile(bufDescTable[p].file);
	}
  }



delete [] bufPool;

delete [] hashTable;

delete [] bufDescTable;


}




void BufMgr::advanceClock()
{

  clockHand = (clockHand+1) % numBufs;

}


void BufMgr::allocBuf(FrameId & frame) 
{	
	FrameId flag=0;				//variable used to check BufferExceededException() error
	while(true)
	{
		advanceClock();
		if(bufDescTable[clockHand].valid)
		{
			if(bufDescTable[clockHand].refbit==true)
			{
				bufDescTable[clockHand].refbit = false;
				continue;
		
			}		
			else
			{
				if(bufDescTable[clockHand].pinCnt >0)
				{	flag++;
						
					if(flag==numBufs)
                				throw BufferExceededException();					

					continue;
				}
				else
				{
					if( bufDescTable[clockHand].dirty==true )
					{
						bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
					}
 					hashTable->remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
                                        bufDescTable[clockHand].Clear();
                                        frame = clockHand;
					break;
				}			
			}		
		}		
		else
		{
			bufDescTable[clockHand].Set(bufDescTable[clockHand].file ,bufDescTable[clockHand].pageNo);
			frame = clockHand;
			break;
		}	
	}


}











	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{

	if(hashTable->lookup (file, pageNo, frno))
	{
		bufDescTable[frno].refbit=true;
 		bufDescTable[frno].pinCnt++;
 		page= &bufPool[frno];

	}
	else
	{
		allocBuf(frno);
		bufPool[frno] = file->readPage(pageNo);
		hashTable->insert(file,pageNo,frno);
		bufDescTable[frno].Set(file, pageNo);
		page = &bufPool[frno];
	}


}








void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
int flag = 1;//flag remains as 1 as long as the HashNotFoundException() is not encountered

try 
{
hashTable->lookup (file, pageNo, frno);
}
catch (HashNotFoundException q)
{
flag = 0;
}


if((flag==1)&(bufDescTable[frno].pinCnt == 0))
  {
  throw PageNotPinnedException(bufDescTable[frno].file->filename(),bufDescTable[frno].pageNo,bufDescTable[frno].frameNo); 
  }


if((dirty == true)&&(flag==1))
        bufDescTable[frno].dirty = true;

if((flag==1)&(bufDescTable[frno].pinCnt > 0))
        bufDescTable[frno].pinCnt--;



}


















void BufMgr::flushFile(const File* file) 
{
	for(FrameId p = 0; p< numBufs;p++)
	if(bufDescTable[p].file == file)
	{
		if( bufDescTable[p].dirty==true )
		{
			bufDescTable[p].file->writePage(bufPool[p]);
			bufDescTable[p].dirty=false;
		}
	
		if(bufDescTable[p].pinCnt > 0)
			throw PagePinnedException("pagePinned",bufDescTable[p].pageNo,p);

		if(!bufDescTable[p].valid)
			throw BadBufferException(p,bufDescTable[p].dirty,bufDescTable[p].valid,bufDescTable[p].refbit);

		hashTable->remove(bufDescTable[p].file,bufDescTable[p].pageNo);
		bufDescTable[p].Clear();
	}


}






void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{

Page p = file->allocatePage();
pageNo = p.page_number();
allocBuf(frno);
bufPool[frno] = p;
bufDescTable[frno].Set(file,pageNo);
hashTable->insert(file,pageNo,frno);
page = &bufPool[frno];

	
}	















void BufMgr::disposePage(File* file, const PageId PageNo)
{

        for(FrameId p = 0; p< numBufs;p++)
	{
        	if((bufDescTable[p].file == file)&&(bufDescTable[p].pageNo == PageNo))
        	{
			if(bufDescTable[p].pinCnt > 0)
				throw PagePinnedException("pagePinned", PageNo, p);
		
			bufDescTable[p].Clear();
			hashTable->remove(file,PageNo);    
		}
	}
	file->deletePage(PageNo);

}





void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	    tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
