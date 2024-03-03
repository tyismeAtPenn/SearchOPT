const limit = 3;
let total = 0;



function autocomplete(input, list) {
  let suggestions;
  input.addEventListener('input', function () {
      closeList();

      //If the input is empty, exit the function
      if (!this.value)
          return;
      suggestions = document.createElement('div');
      suggestions.setAttribute('id', 'suggestions');
      this.parentNode.appendChild(suggestions);
      
      suggestions.style.top = input.offsetTop + input.offsetHeight + 'px'; // Position the suggestions below the input field
      suggestions.style.left = input.offsetLeft + 'px';

      let suggestionCnt = 0;

      for (let i=0; i<list.length; i++) {
          if (suggestionCnt >= 10) break;
          if (list[i].toUpperCase().startsWith(this.value.toUpperCase())) {
              suggestion = document.createElement('div');
              suggestion.innerHTML = list[i];
              
              suggestion.addEventListener('click', function () {
                  input.value = this.innerHTML;
                  closeList();
              });
              suggestion.style.cursor = 'pointer';

              suggestions.appendChild(suggestion);
              suggestionCnt ++;
          }
      }

  });

  function closeList() {
      // let suggestions = document.getElementById('suggestions');
      // if (suggestions)
      //     suggestions.parentNode.removeChild(suggestions);
      if (suggestions) {
        suggestions.innerHTML = '';
        suggestions.parentNode.removeChild(suggestions);
        suggestions = undefined;  // Reset suggestions to undefined
    }
  }
}
autocomplete(document.getElementById('input'), wordDict);

function search(form) {
  const query = form.q.value.toLowerCase();
  fetch(`/query?query=${query}`).then(response => response.text()).then(response => {
    const result = JSON.parse(response);
    return result;
  }).then(urls => {
    const promises = urls.map(url => {
      return fetch(`/process?url=${url.url}&query=${query}`)
        .then(response => {
          response = response.json();
          console.log(response);
        }
          // response.json()\
          )
        .then(data => ({
          title: data?.title,
          url: data?.url,
          description: data?.description
        }));
    });
    return Promise.all(promises);
  })
    .then(results => {
      total = results.length;
      displayResults(results, 1, 2);
    })
    .catch(err => console.error(err));
  return false;
}


const hideLoader = () => {
  loader.classList.remove('show');
};

const showLoader = () => {
  loader.classList.add('show');
};

const hasMorefacts = (page, limit, total) => {
  const startIndex = (page - 1) * limit + 1;
  return total === 0 || startIndex < total;
};


function displayResults(result, page, pageSize) {
  const startIdx = (page - 1) * pageSize;
  const endIdx = startIdx + pageSize;
  const pagResults = result.slice(startIdx, endIdx);
  let currentPage = page;
  const resultDiv = document.getElementById('results');
  resultDiv.innerHTML = '';

  if (pagResults.length === 0) {
    resultDiv.textContent = 'No results found';
  } else {
    const ul = resultDiv.querySelector('ul') || document.createElement('ul');
    pagResults.forEach((res) => {
      if (res.title != null) {
        const li = document.createElement('li');

        //title part
        const title = document.createElement('h4');
        const a = document.createElement('a');
        a.textContent = res.title;
        console.log("res.url:  " + res.url);
        a.href = res.url;
        title.appendChild(a);
        li.appendChild(title);

        //descriotion part
        const descrip = document.createElement('p');
        descrip.textContent = res.description;
        li.appendChild(descrip);

        ul.appendChild(li);
      }
    });
    if (!resultDiv.querySelector('ul')) {
      resultDiv.appendChild(ul);
    }
    // resultDiv.appendChild(ul);
    if (result.length > pageSize) {
      window.addEventListener('scroll', () => {
        const {
          scrollTop,
          scrollHeight,
          clientHeight
        } = document.documentElement;

        if (scrollTop + clientHeight >= scrollHeight - 5 &&
          hasMorefacts(currentPage, limit, total)) {
          currentPage++;
          displayResults(result, currentPage, pageSize);
        }
      });
      // displayResults(result, currentPage + 1, pageSize);
    }
  }
}