package co.id.lsa.proc

/**
  * Created by harji on 3/8/16.
  */
class Customer(var customerId: Int, var zip: String) {

  def this(customerId: Int){
    this(customerId,"")
  }


  def getCustomerId() = customerId

  def setCustomerId(cust: Int): Unit = {
    customerId = cust
  }

  def getZip() =zip

  def setZip(zip: String): Unit ={
    this.zip = zip
  }

  def myFirstMethod():String ={
    "This is my string"
  }


}
