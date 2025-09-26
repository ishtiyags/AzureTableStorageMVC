using System;
using System.Threading.Tasks;
using System.Web.Mvc;
using YourApp.Models; // TableManager & Customer
using Microsoft.WindowsAzure.Storage; // for StorageException handling if needed

namespace ABC_App.Models.Controllers
{
    public class CustomerController : Controller
    {
        private readonly TableManager tableManager;

        public CustomerController()
        {
            // instantiate TableManager that reads the connection string from Web.config
            tableManager = new TableManager("Customers");
        }

        // GET: /Customer
        public async Task<ActionResult> Index()
        {
            var customers = await tableManager.RetrieveEntitiesAsync<Customer>();
            return View(customers);
        }

        // GET: /Customer/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: /Customer/Create
        [HttpPost, ValidateAntiForgeryToken]
        public async Task<ActionResult> Create([Bind(Include = "Name,Email")] Customer model)
        {
            if (!ModelState.IsValid) return View(model);

            // choose sensible partition strategy - we use a constant here
            model.PartitionKey = "CUSTOMERS";
            model.RowKey = Guid.NewGuid().ToString();

            try
            {
                // InsertOrMerge is safe for form updates (avoids needing ETag)
                await tableManager.InsertOrMergeEntityAsync(model);
            }
            catch (StorageException ex)
            {
                // log / show friendly error
                ModelState.AddModelError("", "Storage error: " + ex.Message);
                return View(model);
            }

            return RedirectToAction("Index");
        }

        // GET: /Customer/Edit?partitionKey=...&rowKey=...
        public async Task<ActionResult> Edit(string partitionKey, string rowKey)
        {
            if (string.IsNullOrEmpty(partitionKey) || string.IsNullOrEmpty(rowKey))
                return new HttpStatusCodeResult(400);

            var entity = await tableManager.RetrieveByKeysAsync<Customer>(partitionKey, rowKey);
            if (entity == null) return HttpNotFound();

            return View(entity);
        }

        // POST: /Customer/Edit
        [HttpPost, ValidateAntiForgeryToken]
        public async Task<ActionResult> Edit(Customer model)
        {
            if (!ModelState.IsValid) return View(model);

            try
            {
                // Quick approach: InsertOrMerge updates without needing ETag
                await tableManager.InsertOrMergeEntityAsync(model);

                // Alternative (strict concurrency):
                // await tableManager.ReplaceEntityAsync(model); // requires model.ETag to match
            }
            catch (StorageException ex)
            {
                ModelState.AddModelError("", "Storage error: " + ex.Message);
                return View(model);
            }

            return RedirectToAction("Index");
        }

        // GET: /Customer/Delete?partitionKey=...&rowKey=...
        public async Task<ActionResult> Delete(string partitionKey, string rowKey)
        {
            var entity = await tableManager.RetrieveByKeysAsync<Customer>(partitionKey, rowKey);
            if (entity == null) return HttpNotFound();
            return View(entity);
        }

        // POST: /Customer/Delete  (ActionName "Delete" maps this to POST /Customer/Delete)
        [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
        public async Task<ActionResult> DeleteConfirmed(string partitionKey, string rowKey)
        {
            var entity = await tableManager.RetrieveByKeysAsync<Customer>(partitionKey, rowKey);
            if (entity == null) return HttpNotFound();

            // unconditional delete
            entity.ETag = "*";
            await tableManager.DeleteEntityAsync(entity);

            return RedirectToAction("Index");
        }

        // GET: /Customer/Details?partitionKey=...&rowKey=...
        public async Task<ActionResult> Details(string partitionKey, string rowKey)
        {
            var entity = await tableManager.RetrieveByKeysAsync<Customer>(partitionKey, rowKey);
            if (entity == null) return HttpNotFound();
            return View(entity);
        }
    }
}
