from django.shortcuts import render, redirect
from django.conf import settings
from django.views.decorators.http import require_POST
import sqlite3
import random
import json

from itertools import product

def init_tables():
  con = sqlite3.connect(settings.DATA)
  cur = con.cursor()

  query = (
      f"""
      CREATE VIEW IF NOT EXISTS all_data AS
      SELECT 
          R.rowid AS resp_id, R.model, R.temperature, P.template_name, 
          P.problem_id, P.template_id, P.sys_id, F.problem, F.answer, 
          P.prompt_text, R.response, F.n as n_sents, C.dataset
      FROM responses R
      JOIN prompts P on R.prompt_id=P.rowid
      JOIN fitb_problems F on P.problem_id=F.rowid
      JOIN source_data C on F.ref_id=C.rowid
      """
  )
  cur.execute(query)

  cur.execute(
    f"""
    CREATE TABLE IF NOT EXISTS anno_progress (
      problem_id INTEGER PRIMARY KEY,
      done INTEGER DEFAULT 0,
      num_evaluated INTEGER DEFAULT 0,
      dataset TEXT,
      n_sents INTEGER,
      FOREIGN KEY (problem_id) REFERENCES fitb_problems(rowid)
    );
    """
  )

  cur.execute(
    f"""
    CREATE TABLE IF NOT EXISTS annotations (
      annotator TEXT,
      response_id INTEGER,
      problem_id INTEGER,
      has_guess INTEGER DEFAULT 0,
      num_sentences_correct INTEGER DEFAULT 0,
      guess TEXT,
      coherence INTEGER DEFAULT 0,
      factuality INTEGER DEFAULT 0,
      grammar INTEGER DEFAULT 0,
      no_hallucination INTEGER DEFAULT 0,
      overall INTEGER DEFAULT 0,
      narrative_consistency INTEGER DEFAULT 0,
      no_new_information INTEGER DEFAULT 0,
      no_information_loss INTEGER DEFAULT 0,
      additional_comments TEXT,
      FOREIGN KEY (problem_id) REFERENCES fitb_problems(rowid),
      FOREIGN KEY (response_id) REFERENCES responses(rowid),
      PRIMARY KEY (annotator, response_id)
    );
    """
  )

  con.close()

def select_problem_id():
  exclude_ids = {0, 1, 2, 3, 3001, 4183, 4184, 6911, 6912}

  # use `cycle` to iterate through datasets and number of sentences 
  # (ensures even distribution of annoted data)
  dsets = ['abisee/cnn_dailymail', 'roc', 'sind', 'wikipedia']
  nsents = [1, 2, 3]
  cycle = list(product(dsets, nsents))

  con = sqlite3.connect(settings.DATA)
  cur = con.cursor()
  progress_entries, = cur.execute(f'SELECT COUNT(*) FROM anno_progress').fetchone()
  if progress_entries == 0:
    """ creates the first entry of the anno_progress table """
    print('Creating first entry in anno_progress table...')
    dset, nsent = cycle[0]
    ids, = zip(*cur.execute(f'SELECT rowid FROM fitb_problems WHERE dataset=? AND n=?', (dset, nsent)))
    candidates = set(ids) - exclude_ids
    picked_problem_id = random.choice(list(candidates))
    cur.execute(f'INSERT INTO anno_progress (problem_id, dataset, n_sents) VALUES (?, ?, ?)', (picked_problem_id, dset, nsent))
    con.commit()
  else:
    prog_entry = cur.execute(f"SELECT * FROM anno_progress ORDER BY rowid DESC LIMIT 1").fetchone()
    if prog_entry[1] == 0:
      # resume working on current problem id
      print(f'Resuming current problem id ({prog_entry[0]}) ...')
      picked_problem_id = prog_entry[0]
    else:
      """ select next problem id """
      # select next problem set from `cycle` list
      print('Selecting next problem id...', end=' ')
      current_spot = cycle.index((prog_entry[3], prog_entry[4]))
      dset, nsent = cycle[(current_spot + 1) % len(cycle)]

      ids, = zip(*cur.execute(f'SELECT rowid FROM fitb_problems WHERE dataset=? AND n=?', (dset, nsent)))
      finished_ids, = zip(*cur.execute(f'SELECT problem_id FROM anno_progress').fetchall())
      candidates = set(ids) - exclude_ids
      picked_problem_id = random.choice(list(candidates))
      print(f'Picked problem id: {picked_problem_id} (dataset: {dset}, n_sents: {nsent})')
      cur.execute(f'INSERT INTO anno_progress (problem_id, dataset, n_sents) VALUES (?, ?, ?)', (picked_problem_id, dset, nsent))
      con.commit()
  
  con.close()
  return picked_problem_id


# Create your views here.
def annotation(request):

  init_tables()

  con = sqlite3.connect(settings.DATA)
  cur = con.cursor()
  # dsnames, = zip(*cur.execute(f"SELECT DISTINCT dataset FROM source_data"))
  # combines responses, prompts, fitb_problems, and source_data
  # res = cur.execute(query)
  # print(cur.execute(f'SELECT COUNT(*) FROM prompts WHERE problem_id=2').fetchone())

  """ select the problem id to evaluate """
  problem_id = select_problem_id()
  samples_for_id = cur.execute(f'SELECT * FROM all_data where problem_id=?', (problem_id,)).fetchall()
  num_completed, = cur.execute(f'SELECT COUNT(*) FROM annotations WHERE problem_id=?', (problem_id,)).fetchone()
  print(f'Problem ID: {problem_id}, Samples: {len(samples_for_id)}, Completed: {num_completed}')
  if num_completed >= len(samples_for_id):
    raise Exception(f'All samples for problem {problem_id} have been annotated.')

  """ get the problem and response """
  selected_sample = samples_for_id[num_completed]
  fields = [
    'resp_id', 'model', 'temperature', 'template_name', 
    'problem_id', 'template_id', 'sys_id', 'problem', 
    'answer', 'prompt_text', 'response', 'n_sents', 'dataset',
  ]
  sample_dict = dict(zip(fields, selected_sample))
  left, right = sample_dict['problem'].split('______')
  sample_dict['left'] = left.strip()
  sample_dict['right'] = right.strip()

  con.close()
  return render(
    request, 'annotation/annotation.html', 
    {'data': sample_dict, 'data_json': json.dumps(sample_dict)},
  )

@require_POST
def submit_annotation(request):
  ann_fields = [
    'ann_coherence',
    'ann_factuality',
    'ann_grammar',
    'ann_hallucination',
    'ann_narrative_consistency',
    'ann_no_new_information',
    'ann_no_information_loss'
  ]
  data = json.loads(request.POST.get('data', '{}'))
  if not data:
    raise ValueError('No data provided for annotation submission.')

  guess = request.POST.get('guess', '')
  print(request.POST.get('guessSentenceCount'))

  # convert guess sent count
  guess_sent_count = int(request.POST.get('guessSentenceCount', '0'))
  problem_id = data.get('problem_id', None)
  if problem_id is None:
    raise ValueError('Problem ID is missing in the submitted data.')

  new_row = (
    'john',
    data['resp_id'],
    problem_id,
    int(guess != ''),
    guess_sent_count,
    guess,
    int(request.POST.get('ann_coherence', 0)),
    int(request.POST.get('ann_factuality', 0)),
    int(request.POST.get('ann_grammar', 0)),
    int(request.POST.get('ann_hallucination', 0)),
    int(request.POST.get('ann_overall', 0)),
    int(request.POST.get('ann_narrative_consistency', 0)),
    int(request.POST.get('ann_no_new_information', 0)),
    int(request.POST.get('ann_no_information_loss', 0)),
    request.POST.get('additional_comments', ''),  # additional comments
  )

  con = sqlite3.connect(settings.DATA)
  cur = con.cursor()

  cur.execute(
    f"""
    INSERT INTO annotations (
      annotator, response_id, problem_id, has_guess, num_sentences_correct,
      guess, coherence, factuality, grammar, no_hallucination, overall,
      narrative_consistency, no_new_information, no_information_loss,
      additional_comments
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, new_row
  )
  con.commit()


  samples_for_id, = cur.execute(f'SELECT COUNT(*) FROM all_data where problem_id=?', (problem_id,)).fetchone()
  num_completed, = cur.execute(f'SELECT COUNT(*) FROM annotations WHERE problem_id=?', (problem_id,)).fetchone()

  cur.execute(
    f"""
    UPDATE anno_progress
    SET num_evaluated = ?
    WHERE problem_id = ?
    """, (num_completed, problem_id)
  )
  con.commit()

  if num_completed == samples_for_id:
    print(f'All samples for problem {problem_id} have been annotated.')
    cur.execute(f'UPDATE anno_progress SET done=1 WHERE problem_id=?', (problem_id,))
    con.commit()
  elif num_completed > samples_for_id:
    raise Exception(f'Number of completed annotations ({num_completed}) exceeds the number of samples ({samples_for_id}) for problem {problem_id}.')

  con.close()
  return redirect('default')
