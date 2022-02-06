using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using System.Web.Services;
using %companyname%.Bll;
using System.Configuration;
using System.Data.Entity;
using System.Net;
using System.Threading.Tasks;
using System.Xml.Linq;
using %companyname%.Info;
using %companyname%.Models;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using NLog;
using NLog.Fluent;
using CampaignActivity = %companyname%.Models.CampaignActivity;
using List = %companyname%.Models.List;
using Service = %companyname%.EmailEntegrationWs.tr.com.smartmessage.api.Service;

namespace %companyname%.EmailEntegrationWs
{
    /// <summary>
    /// Summary description for EmailService
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    // [System.Web.Script.Services.ScriptService]
    public class EmailService : System.Web.Services.WebService
    {

        public static List<Guid> listMemberIds;

        [WebMethod]
        [Anotar.NLog.LogToErrorOnException]
        public string DistributeEmails(string campaignId)
        {
            Logger logger = LogManager.GetCurrentClassLogger();
            logger.Info("Email hazırlama işlemine başlanmıştır.");

            var ctx = new %companyname%_MSCRMEntities();
            var theTemplateCtx = new SMRC_ODC_EML_Template();
            var convertedParam = Guid.Parse(campaignId);
            var campaignActivity = ctx.CampaignActivities.FirstOrDefault(p => p.ActivityId == convertedParam);

            if (campaignActivity == null)
                throw new Exception(campaignId + " Kampanya Aktivitesi Bulunamadı");

            if (campaignActivity.RegardingObjectId == null)
                throw new Exception(campaignId + " Kampanya Aktivitesinde RegardingObjectId Bulunamadı");

            var templateId = Convert.ToInt64(campaignActivity.Description);
            var temp = theTemplateCtx.TEMPLATEs.FirstOrDefault(p => p.TEMPLATEID == templateId);
            var campaignItems = GetCampaignItems(campaignActivity.RegardingObjectId, campaignActivity, campaignId);
            var newEmailBll = new EmailBll(false);
            var memberTypeOptionSet = newEmailBll.GetOptionSet("list", "createdfromcode");

            var contactOptionMetadata = GetOptionMetadataByLabel(memberTypeOptionSet, "Contact");
            var contactOptionValue = contactOptionMetadata.Value.Value;

            var leadOptionMetadata = GetOptionMetadataByLabel(memberTypeOptionSet, "Lead");
            var leadOptionValue = leadOptionMetadata.Value.Value;

            var accountOptionMetadata = GetOptionMetadataByLabel(memberTypeOptionSet, "Account");
            var accountOptionValue = accountOptionMetadata.Value.Value;

            // önce campaignitem ve sonra da listmemberid ler için döngüler
            IterateItems(contactOptionValue, leadOptionValue, accountOptionValue, newEmailBll, campaignItems, temp, campaignActivity, campaignId);

            logger.Info("Email hazırlama işlemi sonlanmıştır.");
            return "Gönderilecek email hazırlandı";
        }

        private void IterateItems(int contactOptionValue, int leadOptionValue, int accountOptionValue, EmailBll newEmailBll, List<CampaignItemBase> campaignItems, TEMPLATE temp, CampaignActivity campaignActivity, string campaignId)
        {
            var ctx = new %companyname%_MSCRMEntities();
            // db'e daha az gitmek için contact, account, lead hepsi burada çekilir
            var contactList = ctx.ContactBases.ToList();
            var accountList = ctx.AccountBases.ToList();
            var leadList = ctx.LeadBases.ToList();
            Parallel.ForEach(campaignItems,
            campaignItem =>
            {
                var marketingList = ctx.Lists.FirstOrDefault(a => a.ListId == campaignItem.EntityId);
                if (marketingList == null)
                    throw new Exception(campaignItem.EntityId + " Marketing List Bulunamadı");

                var baseEmailText = campaignActivity.Description;
                if (string.IsNullOrEmpty(baseEmailText))
                    throw new Exception(marketingList.ListId + "Description alanı boş");

                if (marketingList.CreatedFromCode != contactOptionValue &&
                    marketingList.CreatedFromCode != leadOptionValue &&
                    marketingList.CreatedFromCode != accountOptionValue)
                {
                    throw new Exception(marketingList.ListId +
                                        " Id'li Marketing List'in Member Tipi (CreatedFromCode) Anlaşılamadı");
                }
                // kullanılacak listmemberId leri set edilir.
                SetListMemberIds(marketingList, contactOptionValue, leadOptionValue, accountOptionValue, newEmailBll, ctx);

                Parallel.ForEach(listMemberIds, listMemberId =>
                {
                    try
                    {
                        var emailActivity = new Email
                        {
                            RegardingObjectId = new EntityReference("campaignactivity", Guid.Parse(campaignId))
                        };
                        ActivityParty vToActivtyparty = new ActivityParty();

                        if (marketingList.CreatedFromCode == contactOptionValue)
                        {
                            var contact =
                                contactList.FirstOrDefault(
                                    a => a.ContactId.ToString() == listMemberId.ToString());
                            if (contact == null)
                                throw new Exception(listMemberId +
                                                    " Id'li Contact Bulunamadı. Marketing List Id: " +
                                                    marketingList.ListId);

                            vToActivtyparty.PartyId = new EntityReference("contact", contact.ContactId);
                            emailActivity.ToRecipients = contact.EMailAddress1;

                            if (contact.DoNotEMail.Value == true || contact.DoNotBulkEMail.Value == true)
                                return;
                        }
                        else if (marketingList.CreatedFromCode == leadOptionValue)
                        {
                            var lead =
                                leadList.FirstOrDefault(a => a.LeadId.ToString() == listMemberId.ToString());
                            if (lead == null)
                                throw new Exception(listMemberId +
                                                    " Id'li Lead Bulunamadı. Marketing List Id: " +
                                                    marketingList.ListId);

                            vToActivtyparty.PartyId = new EntityReference("lead", lead.LeadId);
                            emailActivity.ToRecipients = lead.EMailAddress1;
                            if (lead.DoNotEMail.Value == true || lead.DoNotBulkEMail.Value == true)
                                return;
                        }
                        else if (marketingList.CreatedFromCode == accountOptionValue)
                        {
                            var account =
                                accountList.FirstOrDefault(
                                    a => a.AccountId.ToString() == listMemberId.ToString());
                            if (account == null)
                                throw new Exception(listMemberId +
                                                    " Id'li account Bulunamadı. Marketing List Id: " +
                                                    marketingList.ListId);

                            vToActivtyparty.PartyId = new EntityReference("account", account.AccountId);
                            emailActivity.ToRecipients = account.EMailAddress1;
                            if (account.DoNotEMail.Value == true || account.DoNotBulkEMail.Value == true)
                                return;
                        }
                        else
                            throw new Exception(marketingList.ListId +
                                                " Id'li Marketing List'in Member Tipi (CreatedFromCode) Anlaşılamadı");

                        ActivityParty vFromActivtyparty = new ActivityParty();
                        vFromActivtyparty.PartyId = new EntityReference("systemuser",
                            new Guid("D135CF6A-8841-E511-806E-000C292EE0E6"));
                        emailActivity.From = new List<ActivityParty> { vFromActivtyparty };
                        emailActivity.To = new List<ActivityParty> { vToActivtyparty };
                        emailActivity.Subject = campaignActivity.Subject;
                        emailActivity.Description = temp.CONTENT;

                        // Eğer email doğruysa aktivite olarak kaydet.
                        if (TestEmail.IsEmail(emailActivity.ToRecipients))
                            newEmailBll.Save(emailActivity);
                    }
                    catch (Exception e)
                    {
                        Logger logger = LogManager.GetCurrentClassLogger();
                        logger.Error(e, " catchin içi");
                    }
                });
            });

        }
        // Marketing list'in Type'ına göre belirleme
        private void SetListMemberIds(List marketingList, int contactOptionValue, int leadOptionValue, int accountOptionValue, EmailBll newEmailBll, %companyname%_MSCRMEntities ctx)
        {
            if (marketingList.Type) 
            {
                var listMembers = newEmailBll.GetDataByFetchQuery(marketingList.Query);
                if (listMembers.Entities.Count < 1)
                    throw new Exception(marketingList.ListId +
                                        " Id'li Marketing List'e Ait List Member Bulunamadı");

                listMemberIds = listMembers.Entities.Select(a =>
                {
                    if (marketingList.CreatedFromCode == accountOptionValue)
                        return a.GetAttributeValue<Guid>("accountid");
                    if (marketingList.CreatedFromCode == contactOptionValue)
                        return a.GetAttributeValue<Guid>("contactid");
                    if (marketingList.CreatedFromCode == leadOptionValue)
                        return a.GetAttributeValue<Guid>("leadid");
                    return Guid.Empty;
                }).Where(a => a != Guid.Empty).ToList();
            }
            else
            {
                listMemberIds =
                    ctx.ListMembers.Where(a => a.ListId == marketingList.ListId)
                        .Select(a => a.EntityId)
                        .ToList();
                if (listMemberIds.Count < 1)
                    throw new Exception(marketingList.ListId +
                                        " Id'li Marketing List'e Ait List Member Bulunamadı");
            }
        }

        private OptionMetadata GetOptionMetadataByLabel(OptionMetadataCollection memberTypeOptionSet, string label)
        {
            var optionMetadata = memberTypeOptionSet.FirstOrDefault(
                            a =>
                            {
                                var firstOrDefault = a.Label.LocalizedLabels.FirstOrDefault(b => b.LanguageCode == 1033);
                                return firstOrDefault != null && firstOrDefault.Label == label;
                            });
            if (optionMetadata == null) return null;
            if (optionMetadata.Value == null) return null;
            return optionMetadata;
        }

        private List<CampaignItemBase> GetCampaignItems(Guid? regardingObjectId, CampaignActivity campaignActivity, string campaignId)
        {
            var Ctx = new %companyname%_MSCRMEntities();

            var retVal = Ctx.CampaignItemBases.Where(p => p.CampaignId == campaignActivity.RegardingObjectId && p.EntityType == 4300).ToList();
            if (retVal.Count < 1)
                throw new Exception(campaignId + " Verilen kampanya aktivitesinde item bulunamadı");
            return retVal;
        }

        [WebMethod]
        [Anotar.NLog.LogToErrorOnException]
        public string SendEmail(string campaignId)
        {
                Logger logger = LogManager.GetCurrentClassLogger();
                logger.Info("Email gönderme işlemine başlanmıştır.");
                Guid convertedParam = Guid.Parse(campaignId);

                EmailBll emails = new EmailBll(false);
                // get emails associated with the given campaign activity id
                var allEmails = emails.GetEmails(convertedParam);
                if (allEmails.Count == 0)
                {
                    throw new Exception("verilen kampanya aktivite id si ile ilgili email bulunamadı");
                }

            var service = new tr.com.smartmessage.api.Service {Timeout = 10800000};

            // timeout attribute'u değiştiği için parametre olarak yollanır

            // topluca gönderme
                //SendAll(allEmails, service);

                // yüzer yüzer gönderme
                SendHundredbyHundred(allEmails, service);

                // teker teker gönderme
                //SendOneByOne(allEmails, service);

                logger.Info("Email gönderme işlemi sonlanmıştır.");
                return "Email başarıyla gönderilmiştir.";
        }

        private void SendHundredbyHundred(List<Email> allEmails, tr.com.smartmessage.api.Service service)
        {
            EmailBll emailBllObj = new EmailBll();
            List<Email> packages = new List<Email>();


            for (var i = 0; i < allEmails.Count(); i++)
            {
                packages.Add(allEmails.ElementAt(i));
                if (packages.Count == 100)
                {
                    SendAll(packages, service);
                    packages.Clear();
                }

            }
            if (packages.Count() > 0)
            {
                SendAll(packages, service);
                packages.Clear();
            }
        }

        // Hata kontrolü yapılmak istendiğinde denenbilir. en çok alınan hata -99
        private void SendOneByOne(List<Email> allEmails, tr.com.smartmessage.api.Service service)
        {
            EmailBll emailBllObj = new EmailBll();

            foreach (var element in allEmails)
            {
                try
                {
                    // yollanacak xml in tümü trunk adlı değişkende tutulur
                    var trunk = new StringBuilder();
                    trunk.AppendFormat(
                        "<SENDEML><VERSION>2.2</VERSION><USR>%companyname%_ws2</USR><PWD>%companyname%@24</PWD><JID>c4adce93-3c6e-42e0-9665-a578009fcf9d</JID><SBJ>{0}</SBJ>",
                        allEmails.ElementAt(0).Subject);
                    // hepsine giden mesaj ortak olduğu için direk ilkini al
                    trunk.AppendFormat("<MSG><![CDATA[{0}]]></MSG>", allEmails.ElementAt(0).Description);
                    trunk.Append("<RCPT_LIST>");
                    trunk.Append(CreateReceipt(element.ToRecipients, element.Subject, element.Description, element.Id));
                    trunk.Append("</RCPT_LIST>");
                    trunk.Append("</SENDEML>");
                    var resultString = service.SENDEML(trunk.ToString());
                    var result = XDocument.Parse(resultString);

                    if (result.Root != null)
                    {
                        var elementRtcd = result.Root.Element("RTCD");
                        //1 dönmesi ok demektir, hata yoktur.
                        if (elementRtcd != null && (result.Root != null && elementRtcd.Value != "1"))
                        {
                            Logger logger = LogManager.GetCurrentClassLogger();
                            logger.Info("beklenen kod dönemedi, dönen kod: " + elementRtcd.Value);
                            //xml'in tamamı log a yazılır, boyutu küçük olduğundan -99 hatası gözlemlenebilir
                            logger.Info("icerik: " + trunk);
                            // foreach in başına gider ve bu hatalı xml'i atlar, hataya sebep olan mail adresine mail gitmez
                            continue;
                        }
                    }

                    // herhangi biri eksikse geç, foreach in başına gidip diğerini al.
                    if (result.Root == null) continue;
                    var elementRsp = result.Root.Element("RSP_LIST");
                    if (elementRsp == null) continue;
                    var elements = elementRsp.Elements();

                    foreach (var elementItem in elements)
                    {
                        var elementEid = elementItem.Element("EID");
                        var externalId = "";
                        if (elementEid != null)
                            externalId = elementEid.Value;

                        var elementMsgid = elementItem.Element("MSGID");
                        var theMsgId = "";
                        if (elementMsgid != null)
                            theMsgId = elementMsgid.Value;
                        emailBllObj.ChangeEmailId(Guid.Parse(externalId), theMsgId);
                    }
                }
                catch (WebException e)
                {
                    Logger logger = LogManager.GetCurrentClassLogger();
                    // System.Net.WebException: The request was aborted: The operation has timed out. Exception'ı burda yakalanmaya çalışıldı.
                    logger.Info("WebException time out catched: " + e.Message);
                }
            }
        }

        // Parçalar halinde yollanmazsa -8 (chunk size exceeded) hatası alınır
        private void SendAll(List<Email> allEmails, tr.com.smartmessage.api.Service service)
        {
            var allStrings = new StringBuilder();
            // ortak bir xml olduğu için burada <RCPT_LIST> </RCPT_LIST> tag leri arasına gelecek email adresleri eklenir
            foreach (var element in allEmails)
            {
                allStrings.Append(CreateReceipt(element.ToRecipients, element.Subject, element.Description, element.Id));
            }
            // yollanacak xml in tümü trunk adlı değişkende tutulur
            var trunk = new StringBuilder();
            trunk.AppendFormat("<SENDEML><VERSION>2.2</VERSION><USR>%companyname%_ws2</USR><PWD>%companyname%@24</PWD><JID>c4adce93-3c6e-42e0-9665-a578009fcf9d</JID><SBJ>{0}</SBJ>", allEmails.ElementAt(0).Subject);
            // hepsine giden mesaj ortak olduğu için direk ilkini al
            // CDATA xml içinde html tag i yollayabilmek için eklendi
            trunk.AppendFormat("<MSG><![CDATA[{0}]]></MSG>", allEmails.ElementAt(0).Description);
            trunk.Append("<RCPT_LIST>");
            trunk.Append(allStrings);
            trunk.Append("</RCPT_LIST>");
            trunk.Append("</SENDEML>");

            var resultString = service.SENDEML(trunk.ToString());

            var result = XDocument.Parse(resultString);

            if (result.Root != null)
            {
                var elementRtcd = result.Root.Element("RTCD");
                //1 dönmesi ok demektir, hata yoktur.
                if (elementRtcd != null && (result.Root != null && elementRtcd.Value != "1"))
                {
                    Logger logger = LogManager.GetCurrentClassLogger();
                    logger.Info("beklenen kod dönemedi, dönen kod: " + elementRtcd.Value);
                    // Burada gönderilen xml, log a eklenmiyor çünkü toplam mail sayısı çok fazla olabilir
                }
            }


            EmailBll emailBllobj = new EmailBll(false); 


            var elements = result.Root.Element("RSP_LIST").Elements();
            foreach (var element in elements)
            {
                try{
                var externalId = element.Element("EID").Value;
                var theMsgId = element.Element("MSGID").Value;
                emailBllobj.ChangeEmailId(Guid.Parse(externalId), theMsgId);
                }
                catch (WebException e)
                {
                    Logger logger = LogManager.GetCurrentClassLogger();
                    // System.Net.WebException: The request was aborted: The operation has timed out. Exception'ı burda yakalanmaya çalışıldı.
                    logger.Info("WebException time out catched: " + e.Message);
                }
            }
            
        }

        // Token şart olmasa da dokümanda yazıldığı gibi de kullanıabilir 

        private string CreateReceipt(string email, string subject, string body, Guid incomingGuid)
        {
            var receiptString = new StringBuilder();
            receiptString.Append("<RCPT>");

            receiptString.AppendFormat("<TA>{0}</TA>", email);
            receiptString.AppendFormat("<EID>{0}</EID>", incomingGuid);

            receiptString.Append("</RCPT>");
            return receiptString.ToString();
        }


    }
}


